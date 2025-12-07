import requests
from db_connection import get_connection

API_KEY = "3b13604b-63be-47ce-a594-bca471752359"
BASE_URL_V1 = "https://api.balldontlie.io/v1"
STANDINGS_URL = f"{BASE_URL_V1}/standings"

HEADERS = {
    "Authorization": API_KEY
}


def ensure_team(conn, team_obj: dict):
    """
    Make sure the team exists in DimTeams and is up to date.
    Uses the same pattern we used elsewhere (city, shortname, etc.).
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
            city        = COALESCE(EXCLUDED.city, dimteams.city),
            shortname   = COALESCE(EXCLUDED.shortname, dimteams.shortname);
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


def upsert_standing(conn, standing: dict):
    """
    Insert or update one season/team row into FactTeamStandings.
    """
    team_obj = standing["team"]
    team_id = team_obj["id"]
    season = standing.get("season")

    conference_record = standing.get("conference_record")
    conference_rank   = standing.get("conference_rank")
    division_record   = standing.get("division_record")
    division_rank     = standing.get("division_rank")
    wins              = standing.get("wins")
    losses            = standing.get("losses")
    home_record       = standing.get("home_record")
    road_record       = standing.get("road_record")

    # Ensure team exists in DimTeams first
    ensure_team(conn, team_obj)

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO factteamstandings
            (teamid, season,
             conferencerecord, conferencerank,
             divisionrecord, divisionrank,
             wins, losses,
             homerecord, roadrecord)
        VALUES
            (%s,%s,
             %s,%s,
             %s,%s,
             %s,%s,
             %s,%s)
        ON CONFLICT (teamid, season) DO UPDATE
        SET conferencerecord = EXCLUDED.conferencerecord,
            conferencerank   = EXCLUDED.conferencerank,
            divisionrecord   = EXCLUDED.divisionrecord,
            divisionrank     = EXCLUDED.divisionrank,
            wins             = EXCLUDED.wins,
            losses           = EXCLUDED.losses,
            homerecord       = EXCLUDED.homerecord,
            roadrecord       = EXCLUDED.roadrecord;
    """, (
        team_id,
        season,
        conference_record,
        conference_rank,
        division_record,
        division_rank,
        wins,
        losses,
        home_record,
        road_record,
    ))
    conn.commit()
    cur.close()


def fetch_standings_for_season(season: int):
    """
    Call /v1/standings?season=YYYY and return the data list.
    """
    params = {"season": season}
    print(f"Calling {STANDINGS_URL} with params={params}")
    resp = requests.get(STANDINGS_URL, headers=HEADERS, params=params, timeout=30)
    print("Status:", resp.status_code)
    if resp.status_code != 200:
        print("Body:", resp.text[:300])
        resp.raise_for_status()

    data = resp.json()
    standings = data.get("data", [])
    print(f"Fetched {len(standings)} standings rows for season {season}.")
    return standings


def main():
    # Add whichever seasons you care about
    seasons_to_load = [2022, 2023, 2024, 2025]  # 2023-24 season; add others like 2022, 2024, etc.

    conn = get_connection()

    for season in seasons_to_load:
        print(f"\n===== Loading standings for season {season} =====")
        try:
            rows = fetch_standings_for_season(season)
            for st in rows:
                upsert_standing(conn, st)
            print(f"Finished inserting standings for season {season}.")
        except Exception as e:
            print(f"Error while processing standings for season {season}: {e}")

    conn.close()
    print("\nAll standings loaded into FactTeamStandings for requested seasons.")


if __name__ == "__main__":
    main()
