import requests
from datetime import datetime, timedelta, date
import argparse
from db_connection import get_connection

API_KEY = "3b13604b-63be-47ce-a594-bca471752359"
BASE_URL = "https://api.balldontlie.io/v1"

HEADERS = {
    "Authorization": API_KEY
}

# ------------- API Fetch -----------------


def fetch_games_for_date(target_date_str: str):
    """
    Fetch all games for a given date using cursor-based pagination.

    Uses:
      ?dates[]=YYYY-MM-DD
      meta.next_cursor for paging
    """
    all_games = []
    cursor = None
    page_num = 1

    while True:
        params = {
            "dates[]": target_date_str,
            "per_page": 100,
        }
        if cursor is not None:
            params["cursor"] = cursor

        print(f"Calling {BASE_URL}/games with params={params}")
        resp = requests.get(
            f"{BASE_URL}/games",
            headers=HEADERS,
            params=params,
            timeout=30,
        )
        print("Status:", resp.status_code)
        if resp.status_code != 200:
            print("Body:", resp.text[:300])
            resp.raise_for_status()

        data = resp.json()
        games_batch = data.get("data", [])
        meta = data.get("meta", {}) or {}

        print(f"Fetched {len(games_batch)} games on cursor-page {page_num}. Meta: {meta}")
        all_games.extend(games_batch)

        cursor = meta.get("next_cursor")
        if not cursor:
            print("No next_cursor returned. Reached end of pages for this date.")
            break

        page_num += 1

    return all_games


# ------------- Dimension helpers -----------------


def ensure_calendar(conn, game_dt: date, season: int, postseason: bool):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO dimcalendar (date, year, month, day, week, dayofweek, season, isplayoffs)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (date) DO NOTHING;
        """,
        (
            game_dt,
            game_dt.year,
            game_dt.month,
            game_dt.day,
            game_dt.isocalendar()[1],
            game_dt.isoweekday(),
            str(season),
            postseason,
        ),
    )
    conn.commit()
    cur.close()


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
    cur.execute(
        """
        INSERT INTO dimteams
            (teamid, teamname, abbreviation, conference, division, venue, city, shortname)
        VALUES
            (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (teamid) DO UPDATE
        SET teamname     = EXCLUDED.teamname,
            abbreviation = EXCLUDED.abbreviation,
            conference   = EXCLUDED.conference,
            division     = EXCLUDED.division,
            city         = EXCLUDED.city,
            shortname    = EXCLUDED.shortname;
        -- we leave venue as-is since API doesn't provide it
        """,
        (
            team_id,
            full_name,
            abbr,
            conference,
            division,
            "",       # venue placeholder
            city,
            shortname,
        ),
    )
    conn.commit()
    cur.close()


# ------------- Upsert game -----------------


def upsert_game(conn, game_obj: dict):
    """Insert or update DimGames row from a /games object, using all available fields."""
    game_id = game_obj["id"]

    # 'date' is usually "YYYY-MM-DD" or ISO with time
    raw_date = game_obj.get("date")
    if raw_date is None:
        raise ValueError(f"Game {game_id} has no 'date' field")

    if "T" in raw_date:
        game_dt = datetime.fromisoformat(raw_date.replace("Z", "+00:00")).date()
    else:
        game_dt = datetime.strptime(raw_date, "%Y-%m-%d").date()

    season = game_obj["season"]
    postseason = game_obj.get("postseason", False)

    # datetime tipoff field (can be None)
    raw_datetime = game_obj.get("datetime")
    if raw_datetime:
        game_datetime = datetime.fromisoformat(raw_datetime.replace("Z", "+00:00"))
    else:
        game_datetime = None

    # ensure calendar row
    ensure_calendar(conn, game_dt, season, postseason)

    home_team = game_obj["home_team"]
    visitor_team = game_obj["visitor_team"]

    home_id = home_team["id"]
    away_id = visitor_team["id"]

    # ensure both teams exist
    ensure_team(conn, home_team)
    ensure_team(conn, visitor_team)

    # core fields
    home_score = game_obj.get("home_team_score", 0)
    visitor_score = game_obj.get("visitor_team_score", 0)
    status = game_obj.get("status") or ""
    period = game_obj.get("period") or 0
    time_str = game_obj.get("time") or ""

    # quarter & OT scoring
    home_q1 = game_obj.get("home_q1")
    home_q2 = game_obj.get("home_q2")
    home_q3 = game_obj.get("home_q3")
    home_q4 = game_obj.get("home_q4")
    home_ot1 = game_obj.get("home_ot1")
    home_ot2 = game_obj.get("home_ot2")
    home_ot3 = game_obj.get("home_ot3")
    home_timeouts_remaining = game_obj.get("home_timeouts_remaining")
    home_in_bonus = game_obj.get("home_in_bonus")

    visitor_q1 = game_obj.get("visitor_q1")
    visitor_q2 = game_obj.get("visitor_q2")
    visitor_q3 = game_obj.get("visitor_q3")
    visitor_q4 = game_obj.get("visitor_q4")
    visitor_ot1 = game_obj.get("visitor_ot1")
    visitor_ot2 = game_obj.get("visitor_ot2")
    visitor_ot3 = game_obj.get("visitor_ot3")
    visitor_timeouts_remaining = game_obj.get("visitor_timeouts_remaining")
    visitor_in_bonus = game_obj.get("visitor_in_bonus")

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO dimgames
            (gameid, date, hometeamid, awayteamid, season, gamenumber,
             home_score, visitor_score, status, period,
             datetime, "time", postseason,
             home_q1, home_q2, home_q3, home_q4,
             home_ot1, home_ot2, home_ot3,
             home_timeouts_remaining, home_in_bonus,
             visitor_q1, visitor_q2, visitor_q3, visitor_q4,
             visitor_ot1, visitor_ot2, visitor_ot3,
             visitor_timeouts_remaining, visitor_in_bonus)
        VALUES
            (%s,%s,%s,%s,%s,%s,
             %s,%s,%s,%s,
             %s,%s,%s,
             %s,%s,%s,%s,
             %s,%s,%s,
             %s,%s,
             %s,%s,%s,%s,
             %s,%s,%s,
             %s,%s)
        ON CONFLICT (gameid) DO UPDATE
        SET date          = EXCLUDED.date,
            hometeamid    = EXCLUDED.hometeamid,
            awayteamid    = EXCLUDED.awayteamid,
            season        = EXCLUDED.season,
            home_score    = EXCLUDED.home_score,
            visitor_score = EXCLUDED.visitor_score,
            status        = EXCLUDED.status,
            period        = EXCLUDED.period,
            datetime      = EXCLUDED.datetime,
            "time"        = EXCLUDED."time",
            postseason    = EXCLUDED.postseason,
            home_q1       = EXCLUDED.home_q1,
            home_q2       = EXCLUDED.home_q2,
            home_q3       = EXCLUDED.home_q3,
            home_q4       = EXCLUDED.home_q4,
            home_ot1      = EXCLUDED.home_ot1,
            home_ot2      = EXCLUDED.home_ot2,
            home_ot3      = EXCLUDED.home_ot3,
            home_timeouts_remaining = EXCLUDED.home_timeouts_remaining,
            home_in_bonus = EXCLUDED.home_in_bonus,
            visitor_q1    = EXCLUDED.visitor_q1,
            visitor_q2    = EXCLUDED.visitor_q2,
            visitor_q3    = EXCLUDED.visitor_q3,
            visitor_q4    = EXCLUDED.visitor_q4,
            visitor_ot1   = EXCLUDED.visitor_ot1,
            visitor_ot2   = EXCLUDED.visitor_ot2,
            visitor_ot3   = EXCLUDED.visitor_ot3,
            visitor_timeouts_remaining = EXCLUDED.visitor_timeouts_remaining,
            visitor_in_bonus          = EXCLUDED.visitor_in_bonus;
        """,
        (
            game_id,
            game_dt,
            home_id,
            away_id,
            str(season),
            None,  # gamenumber (optional / not used yet)
            home_score,
            visitor_score,
            status,
            period,
            game_datetime,
            time_str,
            postseason,
            home_q1,
            home_q2,
            home_q3,
            home_q4,
            home_ot1,
            home_ot2,
            home_ot3,
            home_timeouts_remaining,
            home_in_bonus,
            visitor_q1,
            visitor_q2,
            visitor_q3,
            visitor_q4,
            visitor_ot1,
            visitor_ot2,
            visitor_ot3,
            visitor_timeouts_remaining,
            visitor_in_bonus,
        ),
    )
    conn.commit()
    cur.close()


# ------------- CLI date handling -----------------


def get_date_range_from_args():
    """
    Parse --start-date / --end-date from CLI.
    - If both provided, use that range (inclusive).
    - If only one provided, use it for both (single day).
    - If none provided, default to yesterday only (for daily job).
    """
    parser = argparse.ArgumentParser(
        description="Ingest NBA games from balldontlie into DimGames"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date (YYYY-MM-DD). If omitted, defaults to yesterday.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date (YYYY-MM-DD). If omitted, defaults to same as start-date.",
    )

    args = parser.parse_args()

    if args.start_date and args.end_date:
        start_date_str = args.start_date
        end_date_str = args.end_date
    elif args.start_date and not args.end_date:
        start_date_str = args.start_date
        end_date_str = args.start_date
    elif not args.start_date and args.end_date:
        start_date_str = args.end_date
        end_date_str = args.end_date
    else:
        # Default: yesterday only
        yday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        start_date_str = yday
        end_date_str = yday

    return start_date_str, end_date_str


# ------------- Main -----------------


def main():
    # Get date range from CLI (or default to yesterday)
    start_date_str, end_date_str = get_date_range_from_args()
    print(f"Using game date range: {start_date_str} to {end_date_str}")

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()

    conn = get_connection()

    current_date = start_date
    while current_date <= end_date:
        target_date_str = current_date.strftime("%Y-%m-%d")
        print(f"\n===== Loading games for {target_date_str} =====")

        try:
            games = fetch_games_for_date(target_date_str)
            print(f"Got {len(games)} games for {target_date_str}.")

            for g in games:
                upsert_game(conn, g)

            print(f"Finished upserting games for {target_date_str}.")
        except Exception as e:
            print(f"Error while processing games for {target_date_str}: {e}")

        current_date += timedelta(days=1)

    conn.close()
    print("\nAll games upserted into DimGames for selected range.")


if __name__ == "__main__":
    main()
