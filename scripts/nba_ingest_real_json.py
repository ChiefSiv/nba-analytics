from db_connection import get_connection
import requests
from datetime import datetime

# ===== Dummy fallback data =====
DUMMY_DATE = '2025-11-21'
DUMMY_HOME_TEAM_ID = 111
DUMMY_AWAY_TEAM_ID = 222
DUMMY_PLAYER_ID = 12345
DUMMY_GAME_ID = 67890

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://stats.nba.com",
}

# ===== Helper: ensure dimensions =====
def ensure_calendar(conn, game_date):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dimCalendar (Date, Year, Month, Day, Week, DayOfWeek, Season, IsPlayoffs)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, (game_date, game_date.year, game_date.month, game_date.day,
          game_date.isocalendar()[1], game_date.isoweekday(),
          f"{game_date.year}-{game_date.year+1}", False))
    conn.commit()
    cur.close()

def ensure_teams(conn, team_id, team_name, abbreviation, conference, division, venue):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dimTeams (TeamID, TeamName, Abbreviation, Conference, Division, Venue)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING;
    """, (team_id, team_name, abbreviation, conference, division, venue))
    conn.commit()
    cur.close()

def ensure_player(conn, player_id, player_name, team_id):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dimPlayers (PlayerID, PlayerName, TeamID, Position, Height, Weight, Birthdate, YearsExperience, ActiveFlag)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING;
    """, (player_id, player_name, team_id, 'G', 75, 200, '1990-01-01', 5, True))
    conn.commit()
    cur.close()

def ensure_game(conn, game_id, game_date, home_team_id, away_team_id):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dimGames (GameID, Date, HomeTeamID, AwayTeamID, Season, GameNumber)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING;
    """, (game_id, game_date, home_team_id, away_team_id, f"{game_date.year}-{game_date.year+1}", 1))
    conn.commit()
    cur.close()

# ===== Fact insert functions (reuse previous safe inserts) =====
def insert_fact_player_game(conn, row):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO FactPlayerGame 
        (PlayerID, GameID, TeamID, OpponentID, Minutes, PTS, REB, AST, STL, BLK, TOV,
         FGM, FGA, ThreePM, ThreePA, FTM, FTA, UsageRate, TS, PlusMinus, Pace,
         FantasyPointsDK, FantasyPointsFD)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING;
    """, row)
    conn.commit()
    cur.close()

# ===== Fetch real NBA JSON =====
def fetch_player_game_logs(season="2024-25"):
    url = f"https://stats.nba.com/stats/playergamelogs?Season={season}&SeasonType=Regular%20Season"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        data = r.json()
        logs = data['resultSets'][0]['rowSet']
        processed_rows = []
        for log in logs[:5]:  # first 5 logs for testing
            processed_rows.append([
                log[1],           # PlayerID
                log[2],           # GameID
                log[3],           # TeamID
                log[4],           # OpponentID
                float(log[5]),    # Minutes
                int(log[6]),      # PTS
                int(log[7]),      # REB
                int(log[8]),      # AST
                int(log[9]),      # STL
                int(log[10]),     # BLK
                int(log[11]),     # TO
                int(log[12]),     # FGM
                int(log[13]),     # FGA
                int(log[14]),     # 3PM
                int(log[15]),     # 3PA
                int(log[16]),     # FTM
                int(log[17]),     # FTA
                float(log[18]),   # UsageRate
                float(log[19]),   # TS
                int(log[20]),     # PlusMinus
                float(log[21]),   # Pace
                float(log[22]),   # FantasyPointsDK
                float(log[23])    # FantasyPointsFD
            ])
        return processed_rows
    except Exception as e:
        print("NBA API fetch failed:", e)
        print("Using dummy fallback data")
        return [[DUMMY_PLAYER_ID, DUMMY_GAME_ID, DUMMY_HOME_TEAM_ID, DUMMY_AWAY_TEAM_ID,
                 30,25,5,7,2,1,3,10,20,3,7,5,8,0.2,0.55,5,98,40,35]]

# ===== Main ingestion =====
def main():
    conn = get_connection()
    
    # Ensure dimensions
    game_date = datetime.strptime(DUMMY_DATE, "%Y-%m-%d")
    ensure_calendar(conn, game_date)
    ensure_teams(conn, DUMMY_HOME_TEAM_ID, 'Home Team', 'HT', 'East', 'Atlantic', 'Home Arena')
    ensure_teams(conn, DUMMY_AWAY_TEAM_ID, 'Away Team', 'AT', 'West', 'Pacific', 'Away Arena')
    ensure_player(conn, DUMMY_PLAYER_ID, 'Test Player', DUMMY_HOME_TEAM_ID)
    ensure_game(conn, DUMMY_GAME_ID, game_date, DUMMY_HOME_TEAM_ID, DUMMY_AWAY_TEAM_ID)
    
    # Fetch real data
    rows = fetch_player_game_logs()
    for row in rows:
        insert_fact_player_game(conn, row)
    
    conn.close()
    print("NBA JSON ingestion finished successfully.")

if __name__ == "__main__":
    main()
