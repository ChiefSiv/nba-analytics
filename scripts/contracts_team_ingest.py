import requests
from db_connection import get_connection

API_KEY = "3b13604b-63be-47ce-a594-bca471752359"
BASE_URL_V1 = "https://api.balldontlie.io/v1"
TEAM_CONTRACTS_URL = f"{BASE_URL_V1}/contracts/teams"

HEADERS = {
    "Authorization": API_KEY
}


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


def ensure_player(conn, player_obj: dict):
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


def upsert_contract(conn, contract_row: dict):
    contract_id = contract_row["id"]
    player_id = contract_row["player_id"]
    team_id = contract_row["team_id"]
    season = contract_row.get("season")
    cap_hit = contract_row.get("cap_hit")
    total_cash = contract_row.get("total_cash")
    base_salary = contract_row.get("base_salary")
    rank = contract_row.get("rank")

    # ensure dims
    team_obj = contract_row.get("team")
    if team_obj:
        ensure_team(conn, team_obj)

    player_obj = contract_row.get("player")
    if player_obj:
        ensure_player(conn, player_obj)

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO factplayercontracts
            (contractid, playerid, teamid, season,
             caphit, totalcash, basesalary, rank)
        VALUES
            (%s,%s,%s,%s,
             %s,%s,%s,%s)
        ON CONFLICT (contractid) DO UPDATE
        SET playerid   = EXCLUDED.playerid,
            teamid     = EXCLUDED.teamid,
            season     = EXCLUDED.season,
            caphit     = EXCLUDED.caphit,
            totalcash  = EXCLUDED.totalcash,
            basesalary = EXCLUDED.basesalary,
            rank       = EXCLUDED.rank;
    """, (
        contract_id,
        player_id,
        team_id,
        season,
        cap_hit,
        total_cash,
        base_salary,
        rank,
    ))
    conn.commit()
    cur.close()


def fetch_team_contracts(team_id: int, season: int):
    params = {"team_id": team_id, "season": season}
    print(f"Calling {TEAM_CONTRACTS_URL} with params={params}")
    resp = requests.get(TEAM_CONTRACTS_URL, headers=HEADERS, params=params, timeout=30)
    print("Status:", resp.status_code)
    if resp.status_code == 404:
        print("  No contracts for this team/season (404).")
        return []
    if resp.status_code != 200:
        print("Body:", resp.text[:300])
        resp.raise_for_status()

    data = resp.json()
    rows = data.get("data", [])
    print(f"  Fetched {len(rows)} contract rows for team {team_id}, season {season}.")
    return rows


def get_all_team_ids(conn):
    cur = conn.cursor()
    cur.execute("SELECT teamid FROM dimteams ORDER BY teamid;")
    rows = cur.fetchall()
    cur.close()
    return [r[0] for r in rows]


def main():
    # Decide which seasons you care about
    seasons_to_load = [2022, 2023, 2024, 2025]  # add 2024, 2023, etc. if you want

    conn = get_connection()
    team_ids = get_all_team_ids(conn)
    print(f"Found {len(team_ids)} teams in DimTeams.")

    for season in seasons_to_load:
        print(f"\n===== Loading contracts for season {season} =====")
        for team_id in team_ids:
            try:
                rows = fetch_team_contracts(team_id, season)
                for row in rows:
                    upsert_contract(conn, row)
            except Exception as e:
                print(f"Error processing team {team_id}, season {season}: {e}")

    conn.close()
    print("\nAll team contracts loaded into FactPlayerContracts.")


if __name__ == "__main__":
    main()
