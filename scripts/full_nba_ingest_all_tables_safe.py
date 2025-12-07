from db_connection import get_connection
from datetime import date
import requests

# ===== Dummy IDs and constants =====
DUMMY_DATE = '2025-11-21'
DUMMY_HOME_TEAM_ID = 111
DUMMY_AWAY_TEAM_ID = 222
DUMMY_PLAYER_ID = 12345
DUMMY_GAME_ID = 67890

# ===== Helper functions to ensure dimension tables exist =====
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

# ===== Dummy data functions for fact tables =====
def get_dummy_player_game_row():
    return [DUMMY_PLAYER_ID, DUMMY_GAME_ID, DUMMY_HOME_TEAM_ID, DUMMY_AWAY_TEAM_ID,
            30, 25, 5, 7, 2, 1, 3, 10, 20, 3, 7, 5, 8, 0.2, 0.55, 5, 98, 40, 35]

def get_dummy_team_game_row():
    return [DUMMY_HOME_TEAM_ID, DUMMY_GAME_ID, DUMMY_AWAY_TEAM_ID,
            110, 50, 25, 12, 45.0, 35.0, 75.0, 110.5, 105.0, 100.0, True, 5]

def get_dummy_odds_row():
    return [DUMMY_GAME_ID, 'DummyBook', -5.0, -4.5, 220.0, 221.0, -110, 100, 115.0, 105.0]

def get_dummy_player_props_row():
    return [DUMMY_PLAYER_ID, DUMMY_GAME_ID, 'Points', 25.0, -110, -110, 'DummyBook', 'Pending']

def get_dummy_dfs_row():
    return [DUMMY_PLAYER_ID, DUMMY_GAME_ID, 'DK', 5000, 30, 45, 1.5]

def get_dummy_injury_row():
    return [DUMMY_PLAYER_ID, DUMMY_GAME_ID, 'Out', 'Knee', 'Testing dummy injury', '2025-11-21 10:00:00']

# ===== Insert into fact tables =====
def insert_fact_player_game(conn, row):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO FactPlayerGame 
        (PlayerID, GameID, TeamID, OpponentID, Minutes, PTS, REB, AST, STL, BLK, TOV, FGM, FGA, ThreePM, ThreePA, FTM, FTA, UsageRate, TS, PlusMinus, Pace, FantasyPointsDK, FantasyPointsFD)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, row)
    conn.commit()
    cur.close()

def insert_fact_team_game(conn, row):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO FactTeamGame
        (TeamID, GameID, OpponentID, PTS, REB, AST, TOV, FGPercent, ThreePercent, FTPercent, OffRating, DefRating, Pace, WinFlag, Margin)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING;
    """, row)
    conn.commit()
    cur.close()

def insert_fact_odds(conn, row):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO FactOdds
        (GameID, Book, SpreadOpen, SpreadClose, TotalOpen, TotalClose, MLHome, MLAway, ImpliedHome, ImpliedAway)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING;
    """, row)
    conn.commit()
    cur.close()

def insert_fact_player_props(conn, row):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO FactPlayerProps
        (PlayerID, GameID, PropType, Line, OddsOver, OddsUnder, Book, Result)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING;
    """, row)
    conn.commit()
    cur.close()

def insert_fact_dfs(conn, row):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO FactDFS
        (PlayerID, GameID, Site, Salary, ProjectedMinutes, ProjectedFantasyPoints, ValueScore)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING;
    """, row)
    conn.commit()
    cur.close()

def insert_fact_injury(conn, row):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO FactInjuries
        (PlayerID, GameID, Status, InjuryType, Notes, ReportedAt)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING;
    """, row)
    conn.commit()
    cur.close()

# ===== Main ingestion flow =====
def main():
    conn = get_connection()
    
    # Ensure all dimensions exist
    ensure_calendar(conn)
    ensure_teams(conn)
    ensure_player(conn)
    ensure_game(conn)
    
    # Insert dummy data into all fact tables
    insert_fact_player_game(conn, get_dummy_player_game_row())
    insert_fact_team_game(conn, get_dummy_team_game_row())
    insert_fact_odds(conn, get_dummy_odds_row())
    insert_fact_player_props(conn, get_dummy_player_props_row())
    insert_fact_dfs(conn, get_dummy_dfs_row())
    insert_fact_injury(conn, get_dummy_injury_row())
    
    conn.close()
    print("All dummy data inserted successfully.")

if __name__ == "__main__":
    main()
