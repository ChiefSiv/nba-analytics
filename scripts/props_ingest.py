import requests
from datetime import datetime, date, timedelta
from db_connection import get_connection
import argparse

API_KEY = "3b13604b-63be-47ce-a594-bca471752359"
BASE_URL_V2 = "https://api.balldontlie.io/v2"
PROPS_URL = f"{BASE_URL_V2}/odds/player_props"

HEADERS = {
    "Authorization": API_KEY
}


def parse_float_or_none(val):
    if val is None:
        return None
    try:
        return float(val)
    except Exception:
        return None


def parse_timestamp(ts_str: str):
    """
    Convert ISO string like '2025-11-24T23:46:46.653Z' to Python datetime.
    """
    if not ts_str:
        return None
    try:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except Exception:
        return None


def ensure_player_stub(conn, player_id: int):
    """
    Make sure a PlayerID exists in DimPlayers so the FK doesn't fail.
    If the player is already present from players_ingest.py, this is a no-op.
    """
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dimplayers (playerid, playername, activeflag)
        VALUES (%s, %s, TRUE)
        ON CONFLICT (playerid) DO NOTHING;
    """, (
        player_id,
        f"Player {player_id}",
    ))
    conn.commit()
    cur.close()


def upsert_prop(conn, prop: dict):
    """
    Insert/update one row in FactPlayerProps from a player_props entry.
    """
    prop_id = prop["id"]
    game_id = prop["game_id"]
    player_id = prop["player_id"]
    vendor = prop.get("vendor")
    prop_type = prop.get("prop_type")
    line_value = parse_float_or_none(prop.get("line_value"))
    updated_at = parse_timestamp(prop.get("updated_at"))

    market = prop.get("market") or {}
    market_type = market.get("type")

    over_odds = None
    under_odds = None
    milestone_odds = None

    if market_type == "over_under":
        over_odds = market.get("over_odds")
        under_odds = market.get("under_odds")
    elif market_type == "milestone":
        milestone_odds = market.get("odds")

    # ensure player exists so FK doesn't break
    ensure_player_stub(conn, player_id)

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO factplayerprops
            (propid, gameid, playerid, vendor,
             proptype, linevalue, markettype,
             overodds, underodds, milestoneodds,
             updatedat)
        VALUES
            (%s,%s,%s,%s,
             %s,%s,%s,
             %s,%s,%s,
             %s)
        ON CONFLICT (propid) DO UPDATE
        SET gameid        = EXCLUDED.gameid,
            playerid      = EXCLUDED.playerid,
            vendor        = EXCLUDED.vendor,
            proptype      = EXCLUDED.proptype,
            linevalue     = EXCLUDED.linevalue,
            markettype    = EXCLUDED.markettype,
            overodds      = EXCLUDED.overodds,
            underodds     = EXCLUDED.underodds,
            milestoneodds = EXCLUDED.milestoneodds,
            updatedat     = EXCLUDED.updatedat;
    """, (
        prop_id,
        game_id,
        player_id,
        vendor,
        prop_type,
        line_value,
        market_type,
        over_odds,
        under_odds,
        milestone_odds,
        updated_at,
    ))
    conn.commit()
    cur.close()


def fetch_props_for_game(game_id: int):
    """
    Fetch all player props for a given game_id using cursor-based pagination.
    """
    per_page = 100
    cursor_val = None
    page_idx = 1
    all_props = []

    while True:
        params = {
            "game_id": game_id,
            "per_page": per_page,
        }
        if cursor_val is not None:
            params["cursor"] = cursor_val

        print(f"  Calling {PROPS_URL} with params={params}")
        resp = requests.get(PROPS_URL, headers=HEADERS, params=params, timeout=30)
        print("  Status:", resp.status_code)

        if resp.status_code == 404:
            # No props for this game
            print("  No props for this game (404).")
            break

        if resp.status_code != 200:
            print("  Body:", resp.text[:300])
            resp.raise_for_status()

        data = resp.json()
        batch = data.get("data", [])
        meta = data.get("meta", {}) or {}

        print(f"  Fetched {len(batch)} props on cursor-page {page_idx}. Meta: {meta}")

        if not batch:
            print("  No props on this cursor page. Stopping pagination for this game.")
            break

        all_props.extend(batch)

        cursor_val = meta.get("next_cursor")
        if not cursor_val:
            print("  No next_cursor for props. Reached end for this game.")
            break

        page_idx += 1

    return all_props


def get_game_ids_for_date_range(conn, start_date: date, end_date: date):
    """
    Pull all GameID values from DimGames in the given date range.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT gameid
        FROM dimgames
        WHERE date BETWEEN %s AND %s
        ORDER BY date, gameid;
    """, (start_date, end_date))
    rows = cur.fetchall()
    cur.close()
    return [r[0] for r in rows]


def run_props_for_range(conn, start_date: date, end_date: date):
    """
    Core driver: find games in DimGames between start_date and end_date,
    then load props for each game.
    """
    game_ids = get_game_ids_for_date_range(conn, start_date, end_date)
    print(f"Found {len(game_ids)} games in DimGames for {start_date} to {end_date}.")

    for game_id in game_ids:
        print(f"\n===== Loading props for GameID {game_id} =====")
        try:
            props = fetch_props_for_game(game_id)
            print(f"Got {len(props)} props for game {game_id}.")

            for prop in props:
                upsert_prop(conn, prop)

            print(f"Finished inserting props for game {game_id}.")
        except Exception as e:
            print(f"Error while processing props for game {game_id}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Ingest NBA player props into FactPlayerProps.")
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date (YYYY-MM-DD), inclusive. If omitted, defaults to yesterday."
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date (YYYY-MM-DD), inclusive. If omitted, defaults to yesterday."
    )
    args = parser.parse_args()

    if args.start_date and args.end_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    elif args.start_date or args.end_date:
        raise ValueError("You must provide BOTH --start-date and --end-date, or neither.")
    else:
        # Default: yesterday only
        today = date.today()
        start_date = end_date = today - timedelta(days=1)

    if end_date < start_date:
        raise ValueError("end_date cannot be before start_date.")

    conn = get_connection()
    run_props_for_range(conn, start_date, end_date)
    conn.close()
    print("\nAll props loaded into FactPlayerProps for the selected games/date range.")


if __name__ == "__main__":
    main()
