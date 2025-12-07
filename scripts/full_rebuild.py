import subprocess
import sys
import os
from db_connection import get_connection
import psycopg2

# If your Python command is not "python" (e.g., "py" on Windows),
# change this to "py" or the full path.
PYTHON_EXE = "python"

# Absolute path to the folder where THIS file lives (your scripts folder)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# List of ingestion scripts to run, IN ORDER.
# Make sure these filenames exist inside SCRIPT_DIR.
INGEST_SCRIPTS = [
    # Dimensions (optional, but safe to run)
    "players_ingest.py",            # fills DimPlayers with full metadata
    "games_ingest.py",              # fills DimGames (+ ensures DimTeams/DimCalendar)

    # Core facts
    "player_logs_ingest_real.py",   # fills FactPlayerGame for last 3 seasons
    "team_game_aggregate.py",       # builds FactTeamGame from FactPlayerGame

    # Advanced stats / season-level stuff
    "advanced_stats_ingest.py",     # if you created this
    "standings_ingest.py",          # fills FactTeamStandings (or similar)

    # Betting-related
    "odds_ingest.py",               # fills FactOdds
    "props_ingest.py",              # fills FactPlayerProps

    # Other facts
    "injuries_ingest.py",           # fills FactInjuries
    "contracts_team_ingest.py",     # team contracts
    "contracts_aggregate_ingest.py", # aggregate contracts
]

# ------------- Helpers -------------


def run_sql_resets():
    """
    Truncate fact tables and DimGames so we rebuild everything clean.
    If some tables don't exist yet, we just print a warning and continue.
    """
    print("=== Resetting database tables ===")
    conn = get_connection()
    cur = conn.cursor()

    statements = [
        # FACT tables (order doesn't matter because we use CASCADE)
        "TRUNCATE TABLE factplayerprops RESTART IDENTITY CASCADE;",
        "TRUNCATE TABLE factodds RESTART IDENTITY CASCADE;",
        "TRUNCATE TABLE factteamgame RESTART IDENTITY CASCADE;",
        "TRUNCATE TABLE factplayergame RESTART IDENTITY CASCADE;",
        "TRUNCATE TABLE factinjuries RESTART IDENTITY CASCADE;",
        "TRUNCATE TABLE factdfs RESTART IDENTITY CASCADE;",                  # if you have it
        "TRUNCATE TABLE factteamstandings RESTART IDENTITY CASCADE;",        # if you created it
        "TRUNCATE TABLE factplayercontracts RESTART IDENTITY CASCADE;",      # if you created it
        "TRUNCATE TABLE factplayercontractaggregates RESTART IDENTITY CASCADE;",  # if you created it
        "TRUNCATE TABLE factplayeradvanced RESTART IDENTITY CASCADE;",       # if you created it

        # DIM games only (do NOT clear DimTeams/DimPlayers!)
        "TRUNCATE TABLE dimgames RESTART IDENTITY CASCADE;",
    ]

    for stmt in statements:
        try:
            print(f"Running: {stmt.strip()}")
            cur.execute(stmt)
            conn.commit()
        except psycopg2.Error as e:
            # Don't abort the whole reset if one table doesn't exist yet
            print(f"  ⚠ Warning: {e.diag.message_primary} (while running `{stmt.strip()}`)")

    cur.close()
    conn.close()
    print("=== DB reset completed ===\n")


def run_script(script_name: str):
    """
    Run one of your ingestion scripts as a subprocess, from the scripts folder.
    """
    script_path = os.path.join(SCRIPT_DIR, script_name)
    print(f"\n=== Running {script_name} ===")
    try:
        subprocess.run(
            [PYTHON_EXE, script_path],
            check=True,
            cwd=SCRIPT_DIR,  # run with working directory = scripts folder
        )
        print(f"=== {script_name} completed successfully ===")
    except subprocess.CalledProcessError as e:
        print(f"\n❌ {script_name} failed with exit code {e.returncode}")
        sys.exit(1)
    except FileNotFoundError:
        print(f"\n⚠ Could not find script: {script_path}. "
              f"If the name is different on your machine, update INGEST_SCRIPTS.")
        sys.exit(1)


def main():
    print("===========================================")
    print(" NBA Analytics – FULL REBUILD ORCHESTRATOR ")
    print("===========================================\n")
    print(f"SCRIPT_DIR resolved as: {SCRIPT_DIR}\n")

    # 1) Reset tables
    run_sql_resets()

    # 2) Run each ingestion script in order
    for script in INGEST_SCRIPTS:
        run_script(script)

    print("\n✅ Full rebuild finished. Database should now be synchronized.")


if __name__ == "__main__":
    main()
