import requests
from pprint import pprint

API_KEY = "3b13604b-63be-47ce-a594-bca471752359"
HEADERS = {"Authorization": API_KEY}

BASE_URL = "https://api.balldontlie.io/v1"

def get_json(endpoint, params=None):
    url = f"{BASE_URL}/{endpoint}"
    resp = requests.get(url, headers=HEADERS, params=params or {}, timeout=20)
    print(f"\n=== {endpoint} ===")
    print("Status:", resp.status_code)
    if resp.status_code != 200:
        print("Body:", resp.text[:300])
        return None
    data = resp.json()
    # for collection endpoints with data/meta
    if isinstance(data, dict) and "data" in data and data["data"]:
        first = data["data"][0]
        print("First item keys:")
        pprint(sorted(first.keys()))
        return first
    else:
        print("Top-level keys:")
        pprint(sorted(data.keys()))
        return data

def main():
    # Pick a date in your loaded range
    sample_date = "2023-10-25"

    # Core endpoints
    get_json("stats", {"dates[]": sample_date, "per_page": 1})
    get_json("games", {"dates[]": sample_date, "per_page": 1})
    get_json("players", {"per_page": 1})
    get_json("teams", {"per_page": 1})

    # GOAT endpoints – some may require slightly different params
    get_json("season_averages", {"season": 2023, "player_ids[]": 375})
    get_json("advanced_stats", {"dates[]": sample_date, "per_page": 1})
    get_json("boxscores", {"dates[]": sample_date, "per_page": 1})
    get_json("betting_odds", {"dates[]": sample_date, "per_page": 1})
    get_json("player_props", {"dates[]": sample_date, "per_page": 1})
    get_json("injuries", {"dates[]": sample_date, "per_page": 1})
    get_json("team_standings", {"season": 2023, "per_page": 1})

if __name__ == "__main__":
    main()
