import requests
from db_connection import get_connection

API_KEY = "3b13604b-63be-47ce-a594-bca471752359"
BASE_URL = "https://api.balldontlie.io/v1"

HEADERS = {
    "Authorization": API_KEY
}


def parse_height(height_str: str):
    """
    balldontlie height typically like '6-7' for 6 feet 7 inches.
    We'll convert to total inches as INT. If missing or malformed, return None.
    """
    if not height_str:
        return None
    try:
        parts = height_str.split("-")
        feet = int(parts[0])
        inches = int(parts[1]) if len(parts) > 1 else 0
        return feet * 12 + inches
    except Exception:
        return None


def parse_weight(weight_str: str):
    """
    Weight is usually a string like '220'. We'll convert to INT.
    """
    if not weight_str:
        return None
    try:
        return int(weight_str)
    except Exception:
        return None


def ensure_team(conn, team_obj: dict):
    """
    Upsert team into DimTeams based on balldontlie /v1/teams or embedded team object.
    """
    team_id = team_obj["id"]
    full_name = team_obj.get("full_name") or team_obj.get("name") or f"Team {team_id}"
    shortname = team_obj.get("name") or ""
    abbr = team_obj.get("abbreviation") or ""
    conference = team_obj.get("conference") or ""
    division = team_obj.get("division") or ""
    city = team_obj.get("city") or ""

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dimteams
            (teamid, teamname, abbreviation, conference, division, venue, city, shortname)
        VALUES
            (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (teamid) DO UPDATE
        SET teamname    = EXCLUDED.teamname,
            abbreviation= EXCLUDED.abbreviation,
            conference  = EXCLUDED.conference,
            division    = EXCLUDED.division,
            city        = EXCLUDED.city,
            shortname   = EXCLUDED.shortname;
        -- We leave Venue as-is since the API doesn't provide it
    """, (
        team_id,
        full_name,
        abbr,
        conference,
        division,
        "",       # venue placeholder
        city,
        shortname,
    ))
    conn.commit()
    cur.close()

def upsert_player(conn, player_obj: dict):
    """
    Upsert a single player into DimPlayers using /v1/players object.
    """
    player_id = player_obj["id"]
    first = player_obj.get("first_name") or ""
    last = player_obj.get("last_name") or ""
    name = f"{first} {last}".strip()

    team = player_obj.get("team") or {}
    team_id = team.get("id")

    position = player_obj.get("position") or ""
    height_in = parse_height(player_obj.get("height"))
    weight_lb = parse_weight(player_obj.get("weight"))

    country = player_obj.get("country") or ""
    college = player_obj.get("college") or ""
    draft_round = player_obj.get("draft_round")
    draft_number = player_obj.get("draft_number")
    draft_year = player_obj.get("draft_year")
    jersey_number = player_obj.get("jersey_number")

    # Convert draft fields to INT where possible
    def to_int_or_none(v):
        try:
            return int(v) if v is not None else None
        except Exception:
            return None

    draft_round = to_int_or_none(draft_round)
    draft_number = to_int_or_none(draft_number)
    draft_year = to_int_or_none(draft_year)

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
            teamid         = EXCLUDED.teamid,
            position       = EXCLUDED.position,
            height         = EXCLUDED.height,
            weight         = EXCLUDED.weight,
            country        = EXCLUDED.country,
            college        = EXCLUDED.college,
            draft_round    = EXCLUDED.draft_round,
            draft_number   = EXCLUDED.draft_number,
            draft_year     = EXCLUDED.draft_year,
            jersey_number  = EXCLUDED.jersey_number,
            activeflag     = EXCLUDED.activeflag;
    """, (
        player_id,
        name,
        team_id,
        position,
        height_in,
        weight_lb,
        None,        # birthdate not provided by this API
        None,        # yearsexperience not provided
        True,        # treat as active
        country,
        college,
        draft_round,
        draft_number,
        draft_year,
        jersey_number,
    ))
    conn.commit()
    cur.close()


def fetch_all_players(conn):
    """
    Paginate through /v1/players and upsert into DimPlayers.
    Uses cursor-based pagination: meta.next_cursor.
    """
    per_page = 100
    cursor = None
    page_num = 1  # just for logging, not sent to API

    while True:
        params = {
            "per_page": per_page,
        }
        if cursor is not None:
            params["cursor"] = cursor

        print(f"Calling {BASE_URL}/players with params={params}")
        resp = requests.get(f"{BASE_URL}/players", headers=HEADERS, params=params, timeout=20)
        print("Status:", resp.status_code)
        if resp.status_code != 200:
            print("Body:", resp.text[:300])
            resp.raise_for_status()

        data = resp.json()
        players = data.get("data", [])
        meta = data.get("meta", {})

        print(f"Fetched {len(players)} players on page {page_num}. Meta: {meta}")

        if not players:
            print("No players on this cursor page. Stopping pagination.")
            break

        for p in players:
            team = p.get("team")
            if team:
                ensure_team(conn, team)
            upsert_player(conn, p)

        cursor = meta.get("next_cursor")
        if cursor is None:
            print("No next_cursor returned. Reached end of player list.")
            break

        page_num += 1

    print("All players upserted into DimPlayers.")


def main():
    conn = get_connection()
    fetch_all_players(conn)
    conn.close()


if __name__ == "__main__":
    main()
