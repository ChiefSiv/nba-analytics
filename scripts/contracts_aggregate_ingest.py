import requests
from db_connection import get_connection

API_KEY = "3b13604b-63be-47ce-a594-bca471752359"
BASE_URL_V1 = "https://api.balldontlie.io/v1"
AGG_URL = f"{BASE_URL_V1}/contracts/players/aggregate"

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


def ensure_team(conn, team_obj: dict | None):
    if not team_obj:
        return
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
        None,
        None,
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


def upsert_aggregate(conn, agg_row: dict):
    agg_id = agg_row["id"]
    player_id = agg_row["player_id"]
    team_id = agg_row.get("team_id")

    start_year = agg_row.get("start_year")
    end_year = agg_row.get("end_year")
    contract_type = agg_row.get("contract_type")
    contract_status = agg_row.get("contract_status")
    contract_years = agg_row.get("contract_years")
    total_value = agg_row.get("total_value")
    average_salary = agg_row.get("average_salary")
    guaranteed_at_signing = agg_row.get("guaranteed_at_signing")
    total_guaranteed = agg_row.get("total_guaranteed")
    signed_using = agg_row.get("signed_using")
    free_agent_year = agg_row.get("free_agent_year")
    free_agent_status = agg_row.get("free_agent_status")

    notes = agg_row.get("contract_notes")
    if isinstance(notes, list):
        contract_notes = "; ".join(notes)
    else:
        contract_notes = notes  # could be string or None

    # ensure dims
    player_obj = agg_row.get("player")
    if player_obj:
        ensure_player(conn, player_obj)

    team_obj = agg_row.get("team")
    if team_obj:
        ensure_team(conn, team_obj)

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO factplayercontractaggregates
            (aggregateid, playerid, teamid,
             startyear, endyear,
             contracttype, contractstatus, contractyears,
             totalvalue, averagesalary,
             guaranteedatsigning, totalguaranteed,
             signedusing, freeagentyear, freeagentstatus,
             contractnotes)
        VALUES
            (%s,%s,%s,
             %s,%s,
             %s,%s,%s,
             %s,%s,
             %s,%s,
             %s,%s,%s,
             %s)
        ON CONFLICT (aggregateid) DO UPDATE
        SET playerid            = EXCLUDED.playerid,
            teamid              = EXCLUDED.teamid,
            startyear           = EXCLUDED.startyear,
            endyear             = EXCLUDED.endyear,
            contracttype        = EXCLUDED.contracttype,
            contractstatus      = EXCLUDED.contractstatus,
            contractyears       = EXCLUDED.contractyears,
            totalvalue          = EXCLUDED.totalvalue,
            averagesalary       = EXCLUDED.averagesalary,
            guaranteedatsigning = EXCLUDED.guaranteedatsigning,
            totalguaranteed     = EXCLUDED.totalguaranteed,
            signedusing         = EXCLUDED.signedusing,
            freeagentyear       = EXCLUDED.freeagentyear,
            freeagentstatus     = EXCLUDED.freeagentstatus,
            contractnotes       = EXCLUDED.contractnotes;
    """, (
        agg_id,
        player_id,
        team_id,
        start_year,
        end_year,
        contract_type,
        contract_status,
        contract_years,
        total_value,
        average_salary,
        guaranteed_at_signing,
        total_guaranteed,
        signed_using,
        free_agent_year,
        free_agent_status,
        contract_notes,
    ))
    conn.commit()
    cur.close()


def fetch_aggregates_for_player(player_id: int):
    params = {"player_id": player_id}
    print(f"Calling {AGG_URL} with params={params}")
    resp = requests.get(AGG_URL, headers=HEADERS, params=params, timeout=30)
    print("Status:", resp.status_code)
    if resp.status_code == 404:
        print("  No aggregate contracts for this player (404).")
        return []
    if resp.status_code != 200:
        print("Body:", resp.text[:300])
        resp.raise_for_status()

    data = resp.json()
    rows = data.get("data", [])
    print(f"  Fetched {len(rows)} aggregate contract rows for player {player_id}.")
    return rows


def get_players_with_contracts(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT playerid
        FROM factplayercontracts
        ORDER BY playerid;
    """)
    rows = cur.fetchall()
    cur.close()
    return [r[0] for r in rows]


def main():
    conn = get_connection()
    player_ids = get_players_with_contracts(conn)
    print(f"Found {len(player_ids)} players with contracts in FactPlayerContracts.")

    for pid in player_ids:
        try:
            rows = fetch_aggregates_for_player(pid)
            for row in rows:
                upsert_aggregate(conn, row)
        except Exception as e:
            print(f"Error processing player {pid}: {e}")

    conn.close()
    print("\nAll contract aggregates loaded into FactPlayerContractAggregates.")


if __name__ == "__main__":
    main()
