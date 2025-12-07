import requests
from db_connection import get_connection

# Minimal test: fetch one player game log
URL = "https://stats.nba.com/stats/playergamelogs?Season=2024-25&SeasonType=Regular%20Season"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Referer": "https://stats.nba.com"
}

def fetch_one_player():
    response = requests.get(URL, headers=HEADERS)
    data = response.json()
    first_row = data['resultSets'][0]['rowSet'][0]  # first player game log
    return first_row

def insert_test_data(row):
    conn = get_connection()
    cursor = conn.cursor()
    # Adjust indices based on the NBA JSON structure; here are some typical fields
    cursor.execute("""
        INSERT INTO FactPlayerGame (PlayerID, GameID, TeamID, OpponentID, Minutes, PTS)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, (row[1], row[2], row[3], row[4], row[5], row[6]))  # example indices
    conn.commit()
    cursor.close()
    conn.close()
    print("Test data inserted successfully")

if __name__ == "__main__":
    row = fetch_one_player()
    insert_test_data(row)
