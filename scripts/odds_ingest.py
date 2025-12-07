import requests
from datetime import datetime, timedelta, date
from db_connection import get_connection
import argparse

API_KEY = "3b13604b-63be-47ce-a594-bca471752359"
BASE_URL = "https://api.balldontlie.io/v2"
ODDS_URL = f"{BASE_URL}/odds"

HEADERS = {
    "Authorization": API_KEY
}


def parse_float_or_none(val):
    if val is None:
        return None
    try:
        # API sends strings like "-7.5" or "228.5"
        return float(val)
    except Exception:
        return None


def parse_timestamp(ts_str: str):
    """
    Convert ISO string like '2025-10-21T23:46:11.875Z' to Python datetime.
    """
    if not ts_str:
        return None
    try:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except Exception:
        return None


def upsert_odds(conn, odds_obj: dict):
    """
    Insert/update one odds row into FactOdds.
    """
    odds_id = odds_obj["id"]
    game_id = odds_obj["game_id"]
    vendor = odds_obj["vendor"]

    spread_home_val = parse_float_or_none(odds_obj.get("spread_home_value"))
    spread_home_odds = odds_obj.get("spread_home_odds")
    spread_away_val = parse_float_or_none(odds_obj.get("spread_away_value"))
    spread_away_odds = odds_obj.get("spread_away_odds")

    ml_home = odds_obj.get("moneyline_home_odds")
    ml_away = odds_obj.get("moneyline_away_odds")

    total_val = parse_float_or_none(odds_obj.get("total_value"))
    total_over_odds = odds_obj.get("total_over_odds")
    total_under_odds = odds_obj.get("total_under_odds")

    updated_at = parse_timestamp(odds_obj.get("updated_at"))

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO factodds
            (gameid, vendor, oddsid,
             spreadhomevalue, spreadhomeodds,
             spreadawayvalue, spreadawayodds,
             moneylinehomeodds, moneylineawayodds,
             totalvalue, totaloverodds, totalunderodds,
             updatedat)
        VALUES
            (%s,%s,%s,
             %s,%s,
             %s,%s,
             %s,%s,
             %s,%s,%s,
             %s)
        ON CONFLICT (gameid, vendor) DO UPDATE
        SET oddsid            = EXCLUDED.oddsid,
            spreadhomevalue   = EXCLUDED.spreadhomevalue,
            spreadhomeodds    = EXCLUDED.spreadhomeodds,
            spreadawayvalue   = EXCLUDED.spreadawayvalue,
            spreadawayodds    = EXCLUDED.spreadawayodds,
            moneylinehomeodds = EXCLUDED.moneylinehomeodds,
            moneylineawayodds = EXCLUDED.moneylineawayodds,
            totalvalue        = EXCLUDED.totalvalue,
            totaloverodds     = EXCLUDED.totaloverodds,
            totalunderodds    = EXCLUDED.totalunderodds,
            updatedat         = EXCLUDED.updatedat;
    """, (
        game_id,
        vendor,
        odds_id,
        spread_home_val,
        spread_home_odds,
        spread_away_val,
        spread_away_odds,
        ml_home,
        ml_away,
        total_val,
        total_over_odds,
        total_under_odds,
        updated_at,
    ))
    conn.commit()
    cur.close()


def fetch_odds_for_date(target_date_str: str):
    """
    Fetch all odds for a given date using cursor-based pagination.
    """
    per_page = 100
    cursor_val = None
    page_idx = 1
    all_rows = []

    while True:
        params = {
            "per_page": per_page,
            "dates[]": target_date_str
        }
        if cursor_val is not None:
            params["cursor"] = cursor_val

        print(f"Calling {ODDS_URL} with params={params}")
        resp = requests.get(ODDS_URL, headers=HEADERS, params=params, timeout=30)
        print("Status:", resp.status_code)
        if resp.status_code != 200:
            print("Body:", resp.text[:300])
            resp.raise_for_status()

        data = resp.json()
        batch = data.get("data", [])
        meta = data.get("meta", {}) or {}

        print(f"Fetched {len(batch)} odds rows on cursor-page {page_idx}. Meta: {meta}")

        if not batch:
            print("No odds on this cursor page. Stopping pagination for this date.")
            break

        all_rows.extend(batch)

        cursor_val = meta.get("next_cursor")
        if not cursor_val:
            print("No next_cursor for odds. Reached end for this date.")
            break

        page_idx += 1

    return all_rows


def run_odds_range(conn, start_date, end_date):
    """
    Core driver to load odds between start_date and end_date (inclusive).
    """
    current = start_date
    while current <= end_date:
        target_str = current.strftime("%Y-%m-%d")
        print(f"\n===== Loading odds for {target_str} =====")
        try:
            rows = fetch_odds_for_date(target_str)
            print(f"Got {len(rows)} odds rows for {target_str}.")

            for o in rows:
                upsert_odds(conn, o)

            print(f"Finished inserting odds for {target_str}.")
        except Exception as e:
            print(f"Error while processing odds for {target_str}: {e}")

        current += timedelta(days=1)


def main():
    parser = argparse.ArgumentParser(description="Ingest NBA betting odds from balldontlie into FactOdds.")
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
        # Default: load odds for yesterday only
        today = date.today()
        start_date = end_date = today - timedelta(days=1)

    if end_date < start_date:
        raise ValueError("end_date cannot be before start_date.")

    conn = get_connection()
    run_odds_range(conn, start_date, end_date)
    conn.close()
    print("\nAll odds loaded into FactOdds for the selected range.")


if __name__ == "__main__":
    main()
