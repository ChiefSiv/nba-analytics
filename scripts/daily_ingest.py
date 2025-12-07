import subprocess
import sys
import os
import logging
from datetime import date, timedelta

# Change if you normally use "py" instead of "python"
PYTHON_EXE = "python"

# Folder where this file lives (your scripts folder)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(SCRIPT_DIR, "logs")


def setup_logging(job_name: str):
    """
    Configure logging to write both to console and a daily log file.
    Example log path: scripts/logs/daily_ingest_20251206.log
    """
    os.makedirs(LOG_DIR, exist_ok=True)
    today_str = date.today().strftime("%Y%m%d")
    log_path = os.path.join(LOG_DIR, f"{job_name}_{today_str}.log")

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers if script is somehow called multiple times in one process
    logger.handlers.clear()

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    console_handler = logging.StreamHandler(sys.stdout)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(fmt)
    console_handler.setFormatter(fmt)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logging.info(f"Logging initialized. Writing to: {log_path}")


def run_script(script_name: str, extra_args=None):
    """
    Run one of your ingestion scripts as a subprocess from the scripts folder.
    Captures stdout/stderr and writes them into the log.
    extra_args: list of additional CLI args, e.g. ["--start-date", "2025-01-01", "--end-date", "2025-01-02"]
    """
    script_path = os.path.join(SCRIPT_DIR, script_name)
    cmd = [PYTHON_EXE, script_path]
    if extra_args:
        cmd.extend(extra_args)

    logging.info(f"=== Running {script_name} ===")
    logging.info(f"Command: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            cwd=SCRIPT_DIR,
            capture_output=True,
            text=True,
        )

        if result.stdout:
            logging.info(f"[{script_name} STDOUT]\n{result.stdout}")
        if result.stderr:
            logging.warning(f"[{script_name} STDERR]\n{result.stderr}")

        if result.returncode != 0:
            logging.error(
                f"❌ {script_name} failed with exit code {result.returncode}"
            )
            sys.exit(result.returncode)

        logging.info(f"=== {script_name} completed successfully ===")

    except FileNotFoundError:
        logging.error(
            f"⚠ Could not find script: {script_path}. "
            f"If the name is different on your machine, update daily_ingest.py."
        )
        sys.exit(1)


def main():
    setup_logging("daily_ingest")

    logging.info("===========================================")
    logging.info(" NBA Analytics – DAILY INGEST ORCHESTRATOR ")
    logging.info("===========================================\n")
    logging.info(f"SCRIPT_DIR resolved as: {SCRIPT_DIR}\n")

    # ---- Compute your daily window: yesterday + today ----
    today = date.today()
    yesterday = today - timedelta(days=1)

    start_date_str = yesterday.strftime("%Y-%m-%d")
    end_date_str = today.strftime("%Y-%m-%d")

    logging.info(f"Daily ingest date window: {start_date_str} → {end_date_str}")

    # 1) DimPlayers (safe to rerun; upserts metadata)
    run_script("players_ingest.py")

    # 2) DimGames (for yesterday/today only)
    run_script(
        "games_ingest.py",
        ["--start-date", start_date_str, "--end-date", end_date_str],
    )

    # 3) FactPlayerGame (yesterday/today only)
    run_script(
        "player_logs_ingest_real.py",
        ["--start-date", start_date_str, "--end-date", end_date_str],
    )

    # 4) Aggregate to FactTeamGame
    run_script("team_game_aggregate.py")

        # 5) Advanced stats – only for yesterday/today
    run_script(
        "advanced_stats_ingest.py",
        ["--start-date", start_date_str, "--end-date", end_date_str],
    )

    # 6) Standings
    run_script("standings_ingest.py")

    # 7) Odds (for today only)
    run_script(
        "odds_ingest.py",
        ["--start-date", end_date_str, "--end-date", end_date_str],
    )

    # 8) Player props (today only)
    run_script(
        "props_ingest.py",
        ["--start-date", end_date_str, "--end-date", end_date_str],
    )

    # 9) Injuries snapshot
    run_script("injuries_ingest.py")

    # 10) Contracts – occasional but safe to run
    run_script("contracts_team_ingest.py")
    run_script("contracts_aggregate_ingest.py")

    logging.info("\n✅ Daily ingest finished. Database should now be up-to-date for yesterday/today.")


if __name__ == "__main__":
    main()
