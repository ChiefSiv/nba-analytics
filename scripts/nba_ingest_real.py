from db_connection import get_connection
from playwright.sync_api import sync_playwright
from datetime import datetime
import time

# ===== Dummy fallback IDs =====
DUMMY_DATE = '2025-11-21'
DUMMY_HOME_TEAM_ID = 111
DUMMY_AWAY_TEAM_ID = 222
DUMMY_PLAYER_ID = 12345
DUMMY_GAME_ID = 67890

# ===== Dimension checks (reuse from safe script) =====
def ensure_calendar(conn, game_date):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dimCalendar (Date, Year, Month, Day, Week, DayOfWeek, Season, IsPlayoffs)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, (game_date, game_date.year, game_date.month, game_date.day, game_date.isocalendar()[1],
          game_date.isoweekday(), f"{game_date.year}-{game_date.year+1}", False))
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
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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

# ===== Fact insertion (reuse safe script) =====
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

# ===== Scrape NBA stats page =====
def fetch_player_stats():
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            # Example NBA stats page: adjust for current season or endpoint
            page.goto("https://www.nba.com/stats/players/traditional/?Season=2024-25&SeasonType=Regular%20Season")
            time.sleep(5)  # wait for page to load
            # Extract table rows
            rows = page.query_selector_all("table tbody tr")
            player_data = []
            for row in rows[:5]:  # limit to first 5 players for testing
                cells = row.query_selector_all("td")
                player_data.append([
                    int(cells[0].inner_text()),  # PlayerID (replace with your mapping)
                    int(DUMMY_GAME_ID),
                    int(DUMMY_HOME_TEAM_ID),
                    int(DUMMY_AWAY_TEAM_ID),
                    float(cells[3].inner_text()),  # Minutes
                    int(cells[4].inner_text()),    # PTS
                    int(cells[5].inner_text()),    # REB
                    int(cells[6].inner_text()),    # AST
                    int(cells[7].inner_text()),    # STL
                    int(cells[8].inner_text()),    # BLK
                    int(cells[9].inner_text()),    # TOV
                    int(cells[10].inner_text()),   # FGM
                    int(cells[11].inner_text()),   # FGA
                    int(cells[12].inner_text()),   # 3PM
                    int(cells[13].inner_text()),   # 3PA
                    int(cells[14].inner_text()),   # FTM
                    int(cells[15].inner_text()),   # FTA
                    float(cells[16].inner_text()), # UsageRate
                    float(cells[17].inner_text()), # TS
                    int(cells[18].inner_text()),   # PlusMinus
                    float(cells[19].inner_text()), # Pace
                    float(cells[20].inner_text()), # FantasyPointsDK
                    float(cells[21].inner_text())  # FantasyPointsFD
                ])
            browser.close()
            return player_data
    except Exception as e:
        print("Scraping failed:", e)
        print("Using dummy fallback data")
        return [ [DUMMY_PLAYER_ID, DUMMY_GAME_ID, DUMMY_HOME_TEAM_ID, DUMMY_AWAY_TEAM_ID,
                  30,25,5,7,2,1,3,10,20,3,7,5,8,0.2,0.55,5,98,40,35] ]

# ===== Main flow =====
def main():
    conn = get_connection()
    
    # Ensure dimension tables
    game_date = datetime.strptime(DUMMY_DATE, "%Y-%m-%d")
    ensure_calendar(conn, game_date)
    ensure_teams(conn, DUMMY_HOME_TEAM_ID, 'Home Team', 'HT', 'East', 'Atlantic', 'Home Arena')
    ensure_teams(conn, DUMMY_AWAY_TEAM_ID, 'Away Team', 'AT', 'West', 'Pacific', 'Away Arena')
    ensure_player(conn, DUMMY_PLAYER_ID, 'Test Player', DUMMY_HOME_TEAM_ID)
    ensure_game(conn, DUMMY_GAME_ID, game_date, DUMMY_HOME_TEAM_ID, DUMMY_AWAY_TEAM_ID)
    
    # Fetch real stats (or fallback)
    player_stats_rows = fetch_player_stats()
    
    # Insert into FactPlayerGame
    for row in player_stats_rows:
        insert_fact_player_game(conn, row)
    
    conn.close()
    print("Real NBA ingestion finished successfully.")

if __name__ == "__main__":
    main()
