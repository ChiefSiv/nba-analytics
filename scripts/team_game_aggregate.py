from datetime import datetime, date
import argparse
from db_connection import get_connection


def aggregate_team_games(conn, start_date: date | None = None, end_date: date | None = None):
    cur = conn.cursor()

    # If no date range provided -> full rebuild
    if start_date is None and end_date is None:
        print("No date range provided. Performing FULL rebuild of FactTeamGame (TRUNCATE + recompute).")
        cur.execute("TRUNCATE TABLE factteamgame;")
        conn.commit()
    else:
        # Partial rebuild: delete existing rows only for games within the date range
        print(f"Rebuilding FactTeamGame for games between {start_date} and {end_date} (inclusive).")
        cur.execute(
            """
            DELETE FROM factteamgame
            WHERE gameid IN (
                SELECT gameid
                FROM dimgames
                WHERE (%s IS NULL OR date >= %s)
                  AND (%s IS NULL OR date <= %s)
            );
            """,
            (start_date, start_date, end_date, end_date),
        )
        conn.commit()

    # Now (re)aggregate only games in the desired range
    cur.execute(
        """
        WITH games_in_range AS (
            SELECT gameid, date
            FROM dimgames
            WHERE (%s IS NULL OR date >= %s)
              AND (%s IS NULL OR date <= %s)
        ),
        team_totals AS (
            SELECT
                f.gameid,
                f.teamid,
                SUM(f.minutes)          AS minutes,
                SUM(f.pts)              AS pts,
                SUM(f.reb)              AS reb,
                SUM(f.oreb)             AS oreb,
                SUM(f.dreb)             AS dreb,
                SUM(f.ast)              AS ast,
                SUM(f.stl)              AS stl,
                SUM(f.blk)              AS blk,
                SUM(f.pf)               AS pf,
                SUM(f.tov)              AS tov,
                SUM(f.fgm)              AS fgm,
                SUM(f.fga)              AS fga,
                SUM(f.threepm)          AS threepm,
                SUM(f.threepa)          AS threepa,
                SUM(f.ftm)              AS ftm,
                SUM(f.fta)              AS fta,
                SUM(f.fantasypointsdk)  AS fantasypointsdk,
                SUM(f.fantasypointsfd)  AS fantasypointsfd
            FROM factplayergame f
            JOIN games_in_range gir
              ON f.gameid = gir.gameid
            GROUP BY f.gameid, f.teamid
        ),
        joined AS (
            SELECT
                tt.gameid,
                tt.teamid,
                -- opponent from DimGames
                CASE
                    WHEN tt.teamid = g.hometeamid THEN g.awayteamid
                    ELSE g.hometeamid
                END AS opponentid,
                tt.minutes,
                tt.pts,
                tt.reb,
                tt.oreb,
                tt.dreb,
                tt.ast,
                tt.stl,
                tt.blk,
                tt.pf,
                tt.tov,
                tt.fgm,
                tt.fga,
                tt.threepm,
                tt.threepa,
                tt.ftm,
                tt.fta,
                tt.fantasypointsdk,
                tt.fantasypointsfd,
                g.hometeamid,
                g.awayteamid,
                g.home_score,
                g.visitor_score
            FROM team_totals tt
            JOIN dimgames g
              ON tt.gameid = g.gameid
        ),
        final_calc AS (
            SELECT
                j.gameid,
                j.teamid,
                j.opponentid,
                j.minutes,
                j.pts,
                j.reb,
                j.oreb,
                j.dreb,
                j.ast,
                j.stl,
                j.blk,
                j.pf,
                j.tov,
                j.fgm,
                j.fga,
                j.threepm,
                j.threepa,
                j.ftm,
                j.fta,
                j.fantasypointsdk,
                j.fantasypointsfd,
                -- shooting percentages
                CASE WHEN j.fga    > 0 THEN j.fgm::FLOAT    / j.fga    ELSE 0 END AS fgpercent,
                CASE WHEN j.threepa> 0 THEN j.threepm::FLOAT/ j.threepa ELSE 0 END AS threepercent,
                CASE WHEN j.fta    > 0 THEN j.ftm::FLOAT    / j.fta    ELSE 0 END AS ftpercent,
                -- determine team score vs opp score
                CASE
                    WHEN j.teamid = j.hometeamid THEN j.home_score
                    ELSE j.visitor_score
                END AS team_score,
                CASE
                    WHEN j.teamid = j.hometeamid THEN j.visitor_score
                    ELSE j.home_score
                END AS opp_score
            FROM joined j
        )
        INSERT INTO factteamgame
            (teamid, gameid, opponentid,
             pts, reb, oreb, dreb, ast, stl, blk, pf, tov,
             minutes,
             fgm, fga, threepm, threepa, ftm, fta,
             fgpercent, threepercent, ftpercent,
             offrating, defrating, pace,
             fantasypointsdk, fantasypointsfd,
             winflag, margin)
        SELECT
            teamid,
            gameid,
            opponentid,
            pts, reb, oreb, dreb, ast, stl, blk, pf, tov,
            minutes,
            fgm, fga, threepm, threepa, ftm, fta,
            fgpercent, threepercent, ftpercent,
            NULL::FLOAT AS offrating,
            NULL::FLOAT AS defrating,
            NULL::FLOAT AS pace,
            fantasypointsdk, fantasypointsfd,
            (team_score > opp_score) AS winflag,
            (team_score - opp_score) AS margin
        FROM final_calc
        ON CONFLICT (teamid, gameid) DO UPDATE
        SET pts             = EXCLUDED.pts,
            reb             = EXCLUDED.reb,
            oreb            = EXCLUDED.oreb,
            dreb            = EXCLUDED.dreb,
            ast             = EXCLUDED.ast,
            stl             = EXCLUDED.stl,
            blk             = EXCLUDED.blk,
            pf              = EXCLUDED.pf,
            tov             = EXCLUDED.tov,
            minutes         = EXCLUDED.minutes,
            fgm             = EXCLUDED.fgm,
            fga             = EXCLUDED.fga,
            threepm         = EXCLUDED.threepm,
            threepa         = EXCLUDED.threepa,
            ftm             = EXCLUDED.ftm,
            fta             = EXCLUDED.fta,
            fgpercent       = EXCLUDED.fgpercent,
            threepercent    = EXCLUDED.threepercent,
            ftpercent       = EXCLUDED.ftpercent,
            fantasypointsdk = EXCLUDED.fantasypointsdk,
            fantasypointsfd = EXCLUDED.fantasypointsfd,
            winflag         = EXCLUDED.winflag,
            margin          = EXCLUDED.margin;
        """,
        (start_date, start_date, end_date, end_date),
    )

    conn.commit()
    cur.close()
    print("FactTeamGame aggregated successfully from FactPlayerGame + DimGames.")


def get_date_range_from_args():
    """
    Parse --start-date / --end-date from CLI.

    - If both provided, use that range.
    - If only one provided, use that day for both (single day).
    - If none provided, return (None, None) meaning "all games".
    """
    parser = argparse.ArgumentParser(
        description="Aggregate team-level box scores into FactTeamGame"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date (YYYY-MM-DD). If omitted, and no end-date, rebuild all games.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date (YYYY-MM-DD). If omitted, and no start-date, rebuild all games.",
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
        # No dates -> full rebuild
        return None, None


def main():
    start_date, end_date = get_date_range_from_args()

    if start_date is None and end_date is None:
        print("No dates supplied. Will rebuild FactTeamGame for ALL games.")
    else:
        print(f"Will aggregate FactTeamGame for games between {start_date} and {end_date}.")

    conn = get_connection()
    aggregate_team_games(conn, start_date, end_date)
    conn.close()


if __name__ == "__main__":
    main()
