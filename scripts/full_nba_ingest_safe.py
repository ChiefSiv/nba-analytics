from db_connection import get_connection
import requests
from datetime import date

# ===== NBA API Settings =====
PLAYER_LOGS_URL = "https://stats.nba.com/stats/playergamelogs?Season=2024-25&SeasonType=Regular%20Season"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://stats.nba.com",
}

# ===== Dummy IDs for testing =====
DUMMY_DATE = '2025-11-21'
DUMMY_HOME_TEAM_ID = 111
DUMMY_AWAY_TEAM_ID = 222
DUMMY_PLAYER_ID = 12345
DUMMY_GAME_ID = 67890

# ===== Helpers to ensure dimensions exist =====
def ensure_calendar(conn):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dimCalendar (Date, Year, Month, Day, Week, DayOfWeek, Season, IsPlayoffs)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, (DUMMY_DATE, 2025, 11, 21, 47, 5, '2025-26', False))
    conn.commit()
    cur.close()

def ensure_teams(conn):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dimTeams (TeamID, TeamName, Abbreviation, Conference, Division, Venue)
        VALUES 
        (%s, %s, %s, %s, %s, %s),
        (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, (
        DUMMY_HOME_TEAM_ID, 'Home Team', 'HT', 'East', 'Atlantic', 'Home Arena',
        DUMMY_AWAY_TEAM_ID, 'Away Team', 'AT', 'West', 'Pacific', 'Away Arena'
    ))
    conn.commit()
    cur.close()

def ensure_player(conn):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dimPlayers (PlayerID, PlayerName, TeamID, Position, Height, Weight, Birthdate, YearsExperience, ActiveFlag)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, (DUMMY_PLAYER_ID, 'Test Player', DUMMY_HOME_TEAM_ID, 'G', 75, 200, '1990-01-01', 5, True))
    conn.commit()
    cur.close()

def ensure_game(conn):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dimGames (GameID, Date, HomeTeamID, AwayTeamID, Season, GameNumber)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, (DUMMY_GAME_ID, DUMMY_DATE, DUMMY_HOME_TEAM_ID, DUMMY_AWAY_TEAM_ID, '2025-26', 1))
    conn.commit()
    cur.close()

# ===== Fetch NBA API data =====
def fetch_player_log():
    try:
        response = requests.get(PLAYER_LOGS_URL, headers=HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json()
        first_row = data['resultSets'][0]['rowSet'][0]  # first player game log
        print("Fetched real NBA data successfully.")
        return first_row
    except requests.exceptions.RequestException as e:
        print("NBA API request failed:", e)
        print("Using dummy fallback data instead.")
        # Dummy row: [ignore, PlayerID, GameID, TeamID, OpponentID, Minutes, PTS, REB, AST, STL, BLK, TO, ...]
        return [0, DUMMY_PLAYER_ID, DUMMY_GAME_ID, DUMMY_HOME_TEAM_ID, DUMMY_AWAY_TEAM_ID,
                30, 25, 5, 7, 2, 1, 3, 10, 20, 3, 7, 5, 8, 0.2, 0.55, 5, 98, 40, 35]

# ===== Insert into FactPlayerGame =====
def insert_fact_player_game(conn, row):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO FactPlayerGame 
        (PlayerID, GameID, TeamID, OpponentID, Minutes, PTS, REB, AST, STL, BLK, TOV, FGM, FGA, ThreePM, ThreePA, FTM, FTA, UsageRate, TS, PlusMinus, Pace, FantasyPointsDK, FantasyPointsFD)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, row[1:24])  # slice to match expected columns
    conn.commit()
    cur.close()
    print("FactPlayerGame inserted successfully.")

# ===== Main ingestion flow =====
def main():
    conn = get_connection()
    ensure_calendar(conn)
    ensure_teams(conn)
    ensure_player(conn)
    ensure_game(conn)
    row = fetch_player_log()
    insert_fact_player_game(conn, row)
    conn.close()
    print("Ingestion script finished successfully.")

if __name__ == "__main__":
    main()
