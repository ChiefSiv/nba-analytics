import requests
from datetime import datetime, date, timedelta
import argparse
from db_connection import get_connection

API_URL = "https://api.balldontlie.io/v1/stats"

API_KEY = "3b13604b-63be-47ce-a594-bca471752359"

HEADERS = {
    "Authorization": API_KEY
}

# ------------- Helpers -----------------


def parse_minutes(min_str: str) -> float:
    """Convert '34:22' to 34.37 (minutes as float)."""
    if not min_str:
        return 0.0
    try:
        parts = min_str.split(":")
        mins = int(parts[0])
        secs = int(parts[1]) if len(parts) > 1 else 0
        return mins + secs / 60.0
    except Exception:
        return 0.0


def dk_fantasy_points(pts, reb, ast, stl, blk, tov):
    """DraftKings-ish NBA scoring (simplified)."""
    base = (
        pts
        + 1.25 * reb
        + 1.5 * ast
        + 2 * stl
        + 2 * blk
        - 0.5 * tov
    )
    cats_10 = sum(1 for v in (pts, reb, ast, stl, blk) if v >= 10)
    bonus = 0
    if cats_10 >= 2:
        bonus += 1.5  # double double
    if cats_10 >= 3:
        bonus += 3.0  # triple double
    return base + bonus


def fd_fantasy_points(pts, reb, ast, stl, blk, tov):
    """FanDuel-ish NBA scoring (simplified)."""
    return (
        pts
        + 1.2 * reb
        + 1.5 * ast
        + 3 * stl
        + 3 * blk
        - 1 * tov
    )

# ------------- DB upsert helpers -----------------


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
    Make sure the team exists in DimTeams.
    Note: we now also fill city/shortname so it stays consistent with other scripts.
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
        SET teamname    = EXCLUDED.teamname,
            abbreviation= EXCLUDED.abbreviation,
            conference  = EXCLUDED.conference,
            division    = EXCLUDED.division,
            city        = COALESCE(EXCLUDED.city, dimteams.city),
            shortname   = COALESCE(EXCLUDED.shortname, dimteams.shortname);
        """,
        (team_id, full_name, abbr, conference, division, "", city, shortname),
    )
    conn.commit()
    cur.close()


def ensure_player(conn, player_obj: dict, team_id: int):
    """
    Lightweight ensure_player for the stats ingest.
    (DimPlayers also gets fuller data from the dedicated players_ingest script.)
    """
    name = f'{player_obj["first_name"]} {player_obj["last_name"]}'
    position = player_obj.get("position") or ""

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO dimplayers
            (playerid, playername, teamid, position,
             height, weight, birthdate, yearsexperience, activeflag)
        VALUES
            (%s,%s,%s,%s,
             %s,%s,%s,%s,%s)
        ON CONFLICT (playerid) DO UPDATE
        SET playername = EXCLUDED.playername,
            teamid     = EXCLUDED.teamid,
            position   = EXCLUDED.position;
        """,
        (
            player_obj["id"],
            name,
            team_id,
            position,
            None,
            None,
            None,
            None,
            True,
        ),
    )
    conn.commit()
    cur.close()


def ensure_game(conn, game_obj: dict):
    """
    Ensure DimCalendar + DimTeams + DimGames are populated for this game.
    (Games_ingest.py will later enrich DimGames with quarter scores, etc.)
    """
    game_id = game_obj["id"]
    # parse ISO date like "2024-01-20" or "2024-01-20T00:00:00.000Z"
    raw_date = game_obj["date"]
    if "T" in raw_date:
        game_dt = datetime.fromisoformat(raw_date.replace("Z", "+00:00")).date()
    else:
        game_dt = datetime.strptime(raw_date, "%Y-%m-%d").date()

    ensure_calendar(conn, game_dt, game_obj["season"], game_obj.get("postseason", False))

    home_id = game_obj["home_team_id"]
    away_id = game_obj["visitor_team_id"]

    # Ensure teams exist (placeholder if needed)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO dimteams (teamid, teamname, abbreviation, conference, division, venue, city, shortname)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (teamid) DO NOTHING;
        """,
        (home_id, f"Team {home_id}", "", "", "", "", "", ""),
    )
    cur.execute(
        """
        INSERT INTO dimteams (teamid, teamname, abbreviation, conference, division, venue, city, shortname)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (teamid) DO NOTHING;
        """,
        (away_id, f"Team {away_id}", "", "", "", "", "", ""),
    )
    conn.commit()
    cur.close()

    # Insert game row (if not already enriched by games_ingest.py)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO dimgames (gameid, date, hometeamid, awayteamid, season, gamenumber)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (gameid) DO NOTHING;
        """,
        (
            game_id,
            game_dt,
            home_id,
            away_id,
            str(game_obj["season"]),
            None,
        ),
    )
    conn.commit()
    cur.close()
    return game_id, game_dt


def insert_fact_player_game(conn, stat: dict):
    """Insert one row into FactPlayerGame from a balldontlie stat object."""
    player = stat["player"]
    team = stat["team"]
    game = stat["game"]

    player_id = player["id"]
    team_id = team["id"]
    game_id = game["id"]

    # Determine opponent team
    home_id = game["home_team_id"]
    away_id = game["visitor_team_id"]
    opponent_id = away_id if team_id == home_id else home_id

    # Core box score stats
    mins = parse_minutes(stat.get("min") or "0:00")
    pts = stat.get("pts") or 0
    reb = stat.get("reb") or 0
    oreb = stat.get("oreb") or 0
    dreb = stat.get("dreb") or 0
    ast = stat.get("ast") or 0
    stl = stat.get("stl") or 0
    blk = stat.get("blk") or 0
    pf = stat.get("pf") or 0
    tov = stat.get("turnover") or 0

    fgm = stat.get("fgm") or 0
    fga = stat.get("fga") or 0
    threepm = stat.get("fg3m") or 0
    threepa = stat.get("fg3a") or 0
    ftm = stat.get("ftm") or 0
    fta = stat.get("fta") or 0

    # Percentages
    fg_pct = stat.get("fg_pct") or 0.0
    fg3_pct = stat.get("fg3_pct") or 0.0
    ft_pct = stat.get("ft_pct") or 0.0

    # Fantasy scoring
    dk = dk_fantasy_points(pts, reb, ast, stl, blk, tov)
    fd = fd_fantasy_points(pts, reb, ast, stl, blk, tov)

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO factplayergame
        (playerid, gameid, teamid, opponentid, minutes,
         pts, reb, oreb, dreb, ast, stl, blk, pf, tov,
         fgm, fga, threepm, threepa, ftm, fta,
         fg_pct, fg3_pct, ft_pct,
         fantasypointsdk, fantasypointsfd)
        VALUES (%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,
                %s,%s,%s,
                %s,%s)
        ON CONFLICT (playerid, gameid) DO NOTHING;
        """,
        (
            player_id,
            game_id,
            team_id,
            opponent_id,
            mins,
            pts,
            reb,
            oreb,
            dreb,
            ast,
            stl,
            blk,
            pf,
            tov,
            fgm,
            fga,
            threepm,
            threepa,
            ftm,
            fta,
            fg_pct,
            fg3_pct,
            ft_pct,
            dk,
            fd,
        ),
    )
    conn.commit()
    cur.close()

# ------------- API fetch -----------------


def fetch_stats_for_date(target_date_str: str):
    """
    Fetch all stats for a given date using cursor-based pagination.

    Uses:
      ?dates[]=YYYY-MM-DD
      meta.next_cursor for paging
    """
    all_stats = []
    cursor = None
    page_num = 1

    while True:
        params = {
            "per_page": 100,
            "dates[]": target_date_str,
        }
        if cursor is not None:
            params["cursor"] = cursor

        print(f"Calling {API_URL} with params={params}")
        resp = requests.get(API_URL, params=params, headers=HEADERS, timeout=30)
        print("Status code:", resp.status_code)
        print("Raw response (first 300 chars):", resp.text[:300])

        resp.raise_for_status()
        data = resp.json()

        stats_batch = data.get("data", [])
        meta = data.get("meta", {}) or {}

        batch_count = len(stats_batch)
        print(f"Fetched {batch_count} rows on cursor-page {page_num}. Meta: {meta}")

        all_stats.extend(stats_batch)

        # Pagination: balldontlie v1 now uses 'next_cursor'
        cursor = meta.get("next_cursor")
        if not cursor:
            print("No next_cursor returned. Reached end of pages for this date.")
            break

        page_num += 1

    return all_stats

# ------------- CLI date handling -----------------


def get_date_range_from_args():
    """
    Parse --start-date / --end-date from CLI.
    - If both provided, use that range (inclusive).
    - If only one provided, use it for both (single day).
    - If none provided, default to yesterday only (for daily job).
    """
    parser = argparse.ArgumentParser(
        description="Ingest NBA player game stats from balldontlie into FactPlayerGame"
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
    print(f"Using stats date range: {start_date_str} to {end_date_str}")

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()

    conn = get_connection()

    current_date = start_date
    while current_date <= end_date:
        target_date_str = current_date.strftime("%Y-%m-%d")
        print(f"\n===== Loading stats for {target_date_str} =====")

        try:
            stats = fetch_stats_for_date(target_date_str)
            print(f"Got {len(stats)} player stat rows for {target_date_str}.")

            if not stats:
                print(f"No stats returned for {target_date_str} (maybe no games).")
            else:
                for stat in stats:
                    game = stat["game"]
                    team = stat["team"]
                    player = stat["player"]

                    # ensure dimensions
                    game_id, game_dt = ensure_game(conn, game)
                    ensure_team(conn, team)
                    ensure_player(conn, player, team["id"])

                    # insert fact row
                    insert_fact_player_game(conn, stat)

                print(f"Finished inserting stats for {target_date_str}.")
        except Exception as e:
            print(f"Error while processing {target_date_str}: {e}")

        current_date += timedelta(days=1)

    conn.close()
    print("\nAll dates loaded into FactPlayerGame.")


if __name__ == "__main__":
    main()
