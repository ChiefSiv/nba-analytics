import requests
from db_connection import get_connection

API_KEY = "3b13604b-63be-47ce-a594-bca471752359"
BASE_URL = "https://api.balldontlie.io/v1"

HEADERS = {
    "Authorization": API_KEY
}


def ensure_team(conn, team_obj: dict):
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
    """, (
        team_id,
        full_name,
        abbr,
        conference,
        division,
        "",
        city,
        shortname,
    ))
    conn.commit()
    cur.close()


def fetch_all_teams(conn):
    # 30 NBA teams (plus maybe some extras), so one call with per_page=100 is plenty
    params = {
        "per_page": 100
    }
    print(f"Calling {BASE_URL}/teams with params={params}")
    resp = requests.get(f"{BASE_URL}/teams", headers=HEADERS, params=params, timeout=20)
    print("Status:", resp.status_code)
    if resp.status_code != 200:
        print("Body:", resp.text[:300])
        resp.raise_for_status()

    data = resp.json()
    teams = data.get("data", [])
    print(f"Fetched {len(teams)} teams.")

    for team in teams:
        ensure_team(conn, team)

    print("All teams upserted into DimTeams.")


def main():
    conn = get_connection()
    fetch_all_teams(conn)
    conn.close()


if __name__ == "__main__":
    main()
