import requests
from datetime import datetime
from db_connection import get_connection

API_KEY = "3b13604b-63be-47ce-a594-bca471752359"
BASE_URL = "https://api.balldontlie.io/v1"
INJURIES_URL = f"{BASE_URL}/player_injuries"

HEADERS = {
    "Authorization": API_KEY
}


# ---------- Helpers to keep DimPlayers up to date ----------

def parse_height(height_str: str):
    if not height_str:
        return None
    try:
        parts = height_str.split("-")
        feet = int(parts[0])
        inches = int(parts[1]) if len(parts) > 1 else 0
        return feet * 12 + inches
    except Exception:
        return None


def to_int_or_none(v):
    try:
        return int(v) if v is not None else None
    except Exception:
        return None


def ensure_player(conn, player_obj: dict):
    """
    Upsert player into DimPlayers using the injury player's object.
    Similar to what we did in advanced_stats_ingest.
    """
    player_id = player_obj["id"]
    first = player_obj.get("first_name") or ""
    last = player_obj.get("last_name") or ""
    name = f"{first} {last}".strip()

    team_id = player_obj.get("team_id")
    position = player_obj.get("position") or ""
    height_in = parse_height(player_obj.get("height"))
    weight_lb = to_int_or_none(player_obj.get("weight"))
    jersey_number = player_obj.get("jersey_number")
    college = player_obj.get("college")
    country = player_obj.get("country")
    draft_round = to_int_or_none(player_obj.get("draft_round"))
    draft_number = to_int_or_none(player_obj.get("draft_number"))
    draft_year = to_int_or_none(player_obj.get("draft_year"))

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dimplayers
            (playerid, playername, teamid, position,
             height, weight, birthdate, yearsexperience, activeflag,
             country, college, draft_round, draft_number, draft_year, jersey_number)
        VALUES
            (%s,%s,%s,%s,
             %s,%s,%s,%s,%s,
             %s,%s,%s,%s,%s,%s)
        ON CONFLICT (playerid) DO UPDATE
        SET playername     = EXCLUDED.playername,
            teamid         = COALESCE(EXCLUDED.teamid, dimplayers.teamid),
            position       = EXCLUDED.position,
            height         = COALESCE(EXCLUDED.height, dimplayers.height),
            weight         = COALESCE(EXCLUDED.weight, dimplayers.weight),
            country        = COALESCE(EXCLUDED.country, dimplayers.country),
            college        = COALESCE(EXCLUDED.college, dimplayers.college),
            draft_round    = COALESCE(EXCLUDED.draft_round, dimplayers.draft_round),
            draft_number   = COALESCE(EXCLUDED.draft_number, dimplayers.draft_number),
            draft_year     = COALESCE(EXCLUDED.draft_year, dimplayers.draft_year),
            jersey_number  = COALESCE(EXCLUDED.jersey_number, dimplayers.jersey_number),
            activeflag     = COALESCE(dimplayers.activeflag, TRUE);
    """, (
        player_id,
        name,
        team_id,
        position,
        height_in,
        weight_lb,
        None,   # birthdate
        None,   # yearsexperience
        True,
        country,
        college,
        draft_round,
        draft_number,
        draft_year,
        jersey_number,
    ))
    conn.commit()
    cur.close()

    return player_id, team_id


# ---------- FactInjuries insert/refresh ----------

def refresh_injuries(conn):
    """
    Refresh FactInjuries with the current player_injuries snapshot.
    Uses cursor-based pagination.
    """
    cur = conn.cursor()
    # Wipe out old snapshot
    cur.execute("TRUNCATE TABLE factinjuries;")
    conn.commit()
    cur.close()

    per_page = 100
    cursor_val = None
    page_idx = 1

    total_rows = 0

    while True:
        params = {
            "per_page": per_page,
        }
        if cursor_val is not None:
            params["cursor"] = cursor_val

        print(f"Calling {INJURIES_URL} with params={params}")
        resp = requests.get(INJURIES_URL, headers=HEADERS, params=params, timeout=20)
        print("Status:", resp.status_code)
        if resp.status_code != 200:
            print("Body:", resp.text[:300])
            resp.raise_for_status()

        data = resp.json()
        injuries = data.get("data", [])
        meta = data.get("meta", {})

        print(f"Fetched {len(injuries)} injuries on cursor-page {page_idx}. Meta: {meta}")

        if not injuries:
            print("No injuries on this cursor page. Stopping pagination.")
            break

        for inj in injuries:
            player_obj = inj["player"]
            status = inj.get("status")
            return_date = inj.get("return_date")  # string like "Nov 17"
            description = inj.get("description")

            player_id, team_id = ensure_player(conn, player_obj)

            cur = conn.cursor()
            cur.execute("""
                INSERT INTO factinjuries
                    (playerid, teamid, status, returndatetext, description, pulledat)
                VALUES
                    (%s,%s,%s,%s,%s, NOW())
                ON CONFLICT (playerid) DO UPDATE
                SET teamid         = EXCLUDED.teamid,
                    status         = EXCLUDED.status,
                    returndatetext = EXCLUDED.returndatetext,
                    description    = EXCLUDED.description,
                    pulledat       = EXCLUDED.pulledat;
            """, (
                player_id,
                team_id,
                status,
                return_date,
                description,
            ))
            conn.commit()
            cur.close()
            total_rows += 1

        cursor_val = meta.get("next_cursor")
        if not cursor_val:
            print("No next_cursor. Reached end of injury list.")
            break

        page_idx += 1

    print(f"Finished refreshing FactInjuries. Total rows upserted: {total_rows}")


def main():
    conn = get_connection()
    refresh_injuries(conn)
    conn.close()


if __name__ == "__main__":
    main()
