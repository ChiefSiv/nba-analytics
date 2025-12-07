import requests
from db_connection import get_connection

# NBA Stats API URL for Player Game Logs
URL = "https://stats.nba.com/stats/playergamelogs?Season=2024-25&SeasonType=Regular%20Season"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://stats.nba.com",
}

def fetch_one_player():
    try:
        response = requests.get(URL, headers=HEADERS, timeout=10)
        response.raise_for_status()  # Raise an error for HTTP errors
        data = response.json()
        first_row = data['resultSets'][0]['rowSet'][0]  # First player game log
        print("Fetched real NBA data successfully.")
        return first_row
    except requests.exceptions.RequestException as e:
        print("NBA API request failed:", e)
        print("Using dummy fallback data instead.")
        # Fallback dummy data: indices match minimal insert example
        return [0, 12345, 67890, 111, 222, 30, 25]  # [ignore, PlayerID, GameID, TeamID, OpponentID, Minutes, PTS]

def insert_test_data(row):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO FactPlayerGame (PlayerID, GameID, TeamID, OpponentID, Minutes, PTS)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, (row[1], row[2], row[3], row[4], row[5], row[6]))
        conn.commit()
        print("Test data inserted successfully.")
    except Exception as e:
        print("Failed to insert data:", e)
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    row = fetch_one_player()
    insert_test_data(row)
