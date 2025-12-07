import requests
from datetime import datetime, timedelta, date
import argparse
from db_connection import get_connection

API_KEY = "3b13604b-63be-47ce-a594-bca471752359"
BASE_URL = "https://api.balldontlie.io/v1"
ADVANCED_URL = f"{BASE_URL}/stats/advanced"  # path per balldontlie docs

HEADERS = {
    "Authorization": API_KEY
}

# ---------- Dim helpers ----------

def ensure_calendar(conn, game_dt: date, season: int, postseason: bool):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dimcalendar (date, year, month, day, week, dayofweek, season, isplayoffs)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (date) DO NOTHING;
    """, (
        game_dt,
        game_dt.year,
        game_dt.month,
        game_dt.day,
        game_dt.isocalendar()[1],
        game_dt.isoweekday(),
        str(season),
        postseason,
    ))
    conn.commit()
    cur.close()


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


def ensure_game(conn, game_obj: dict):
    """
    Ensure DimGames + DimCalendar rows exist for this game, then return (game_id, game_date).
    advanced_stats.game has a simpler shape.
    """
    game_id = game_obj["id"]
    # date is "YYYY-MM-DD"
    game_dt = datetime.strptime(game_obj["date"], "%Y-%m-%d").date()
    season = game_obj.get("season")
    postseason = game_obj.get("postseason", False)

    ensure_calendar(conn, game_dt, season, postseason)

    home_id = game_obj["home_team_id"]
    away_id = game_obj["visitor_team_id"]

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dimgames
            (gameid, date, hometeamid, awayteamid, season, gamenumber,
             home_score, visitor_score, status, period)
        VALUES
            (%s,%s,%s,%s,%s,%s,
             %s,%s,%s,%s)
        ON CONFLICT (gameid) DO UPDATE
        SET date          = EXCLUDED.date,
            hometeamid    = EXCLUDED.hometeamid,
            awayteamid    = EXCLUDED.awayteamid,
            season        = EXCLUDED.season,
            home_score    = COALESCE(EXCLUDED.home_score, dimgames.home_score),
            visitor_score = COALESCE(EXCLUDED.visitor_score, dimgames.visitor_score),
            status        = COALESCE(EXCLUDED.status, dimgames.status),
            period        = COALESCE(EXCLUDED.period, dimgames.period);
    """, (
        game_id,
        game_dt,
        home_id,
        away_id,
        str(season),
        None,
        game_obj.get("home_team_score"),
        game_obj.get("visitor_team_score"),
        game_obj.get("status"),
        game_obj.get("period"),
    ))
    conn.commit()
    cur.close()

    return game_id, game_dt


def ensure_player(conn, player_obj: dict):
    """
    Minimal ensure for DimPlayers; most enrichment is already done via players_ingest.
    advanced_stats.player includes a team_id.
    """
    player_id = player_obj["id"]
    first = player_obj.get("first_name") or ""
    last = player_obj.get("last_name") or ""
    name = f"{first} {last}".strip()
    position = player_obj.get("position") or ""
    height = player_obj.get("height")
    weight = player_obj.get("weight")
    jersey_number = player_obj.get("jersey_number")
    college = player_obj.get("college")
    country = player_obj.get("country")
    draft_round = player_obj.get("draft_round")
    draft_number = player_obj.get("draft_number")
    draft_year = player_obj.get("draft_year")
    team_id = player_obj.get("team_id")

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

    height_in = parse_height(height)
    weight_lb = to_int_or_none(weight)
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

    return player_id, team_id


# ---------- Fact insert ----------

def upsert_player_advanced(conn, adv: dict):
    """
    Insert/update one row in FactPlayerAdvanced from an advanced_stats entry.
    """
    player_obj = adv["player"]
    team_obj   = adv["team"]
    game_obj   = adv["game"]

    # ensure dims
    ensure_team(conn, team_obj)
    game_id, game_dt = ensure_game(conn, game_obj)
    player_id, team_id_from_player = ensure_player(conn, player_obj)

    # prefer team.id from top-level team object
    team_id = team_obj["id"]

    # advanced metrics
    pie   = adv.get("pie")
    pace  = adv.get("pace")
    ap    = adv.get("assist_percentage")
    ar    = adv.get("assist_ratio")
    a2t   = adv.get("assist_to_turnover")
    drtg  = adv.get("defensive_rating")
    drebp = adv.get("defensive_rebound_percentage")
    efg   = adv.get("effective_field_goal_percentage")
    nrtg  = adv.get("net_rating")
    ortg  = adv.get("offensive_rating")
    orebp = adv.get("offensive_rebound_percentage")
    rebp  = adv.get("rebound_percentage")
    tsp   = adv.get("true_shooting_percentage")
    tor   = adv.get("turnover_ratio")
    usg   = adv.get("usage_percentage")

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO factplayeradvanced
            (playerid, gameid, teamid,
             pie, pace,
             assistpercentage, assistratio, assisttoturnover,
             defensiverating, defensivereboundpercentage,
             effectivefieldgoalpercentage, netrating,
             offensiverating, offensivereboundpercentage,
             reboundpercentage, trueshootingpercentage,
             turnoverratio, usagepercentage)
        VALUES
            (%s,%s,%s,
             %s,%s,
             %s,%s,%s,
             %s,%s,
             %s,%s,
             %s,%s,
             %s,%s,
             %s,%s)
        ON CONFLICT (playerid, gameid) DO UPDATE
        SET teamid                       = EXCLUDED.teamid,
            pie                          = EXCLUDED.pie,
            pace                         = EXCLUDED.pace,
            assistpercentage             = EXCLUDED.assistpercentage,
            assistratio                  = EXCLUDED.assistratio,
            assisttoturnover             = EXCLUDED.assisttoturnover,
            defensiverating              = EXCLUDED.defensiverating,
            defensivereboundpercentage   = EXCLUDED.defensivereboundpercentage,
            effectivefieldgoalpercentage = EXCLUDED.effectivefieldgoalpercentage,
            netrating                    = EXCLUDED.netrating,
            offensiverating              = EXCLUDED.offensiverating,
            offensivereboundpercentage   = EXCLUDED.offensivereboundpercentage,
            reboundpercentage            = EXCLUDED.reboundpercentage,
            trueshootingpercentage       = EXCLUDED.trueshootingpercentage,
            turnoverratio                = EXCLUDED.turnoverratio,
            usagepercentage              = EXCLUDED.usagepercentage;
    """, (
        player_id, game_id, team_id,
        pie, pace,
        ap, ar, a2t,
        drtg, drebp,
        efg, nrtg,
        ortg, orebp,
        rebp, tsp,
        tor, usg,
    ))
    conn.commit()
    cur.close()


# ---------- Fetch from API ----------

def fetch_advanced_for_date(target_date_str: str):
    """
    Fetch all advanced stats for a given date using cursor-based pagination.
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

        print(f"Calling {ADVANCED_URL} with params={params}")
        resp = requests.get(ADVANCED_URL, headers=HEADERS, params=params, timeout=30)
        print("Status:", resp.status_code)
        if resp.status_code != 200:
            print("Body:", resp.text[:300])
            resp.raise_for_status()

        data = resp.json()
        batch = data.get("data", [])
        meta = data.get("meta", {}) or {}

        print(f"Fetched {len(batch)} advanced rows on cursor-page {page_idx}. Meta: {meta}")

        if not batch:
            print("No advanced stats on this cursor page. Stopping pagination for this date.")
            break

        all_rows.extend(batch)

        cursor_val = meta.get("next_cursor")
        if not cursor_val:
            print("No next_cursor for advanced stats. Reached end for this date.")
            break

        page_idx += 1

    return all_rows


# ---------- Date range helper ----------

def get_date_range_from_args():
    """
    Parse --start-date / --end-date.
    If none provided, default to your current backfill window:
    2022-10-01 to 2025-12-05.
    """
    parser = argparse.ArgumentParser(
        description="Ingest advanced NBA stats into FactPlayerAdvanced"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date (YYYY-MM-DD).",
    )
    args = parser.parse_args()

    if args.start_date and args.end_date:
        s = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        e = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        return s, e
    elif args.start_date and not args.end_date:
        d = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        return d, d
    elif not args.start_date and args.end_date:
        d = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        return d, d
    else:
        # default backfill window (matches your earlier config)
        s = datetime.strptime("2022-10-01", "%Y-%m-%d").date()
        e = datetime.strptime("2025-12-05", "%Y-%m-%d").date()
        return s, e


# ---------- Main driver ----------

def main():
    start_date, end_date = get_date_range_from_args()
    print(f"Advanced stats ingest from {start_date} to {end_date}")

    conn = get_connection()

    current = start_date
    while current <= end_date:
        target_str = current.strftime("%Y-%m-%d")
        print(f"\n===== Loading advanced stats for {target_str} =====")
        try:
            rows = fetch_advanced_for_date(target_str)
            print(f"Got {len(rows)} advanced stat rows for {target_str}.")

            for adv in rows:
                upsert_player_advanced(conn, adv)

            print(f"Finished inserting advanced stats for {target_str}.")
        except Exception as e:
            print(f"Error while processing advanced stats for {target_str}: {e}")

        current += timedelta(days=1)

    conn.close()
    print("\nAll advanced stats loaded into FactPlayerAdvanced for the selected range.")


if __name__ == "__main__":
    main()
