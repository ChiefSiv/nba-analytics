import subprocess
import sys
import os
import logging
from datetime import date

# Change if you normally use "py" instead of "python"
PYTHON_EXE = "python"

# Folder where this file lives (your scripts folder)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(SCRIPT_DIR, "logs")


def setup_logging(job_name: str):
    """
    Configure logging to write both to console and a daily log file.
    Example log path: scripts/logs/betting_ingest_20251206.log
    """
    os.makedirs(LOG_DIR, exist_ok=True)
    today_str = date.today().strftime("%Y%m%d")
    log_path = os.path.join(LOG_DIR, f"{job_name}_{today_str}.log")

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
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
    """
    script_path = os.path.join(SCRIPT_DIR, script_name)
    cmd = [PYTHON_EXE, script_path]
    if extra_args:
        cmd.extend(extra_args)

    logging.info(f"\n=== Running {script_name} ===")
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
            f"If the name is different on your machine, update betting_ingest.py."
        )
        sys.exit(1)


def main():
    setup_logging("betting_ingest")

    logging.info("=============================================")
    logging.info(" NBA Analytics – INTRADAY BETTING INGEST JOB ")
    logging.info("=============================================\n")
    logging.info(f"SCRIPT_DIR resolved as: {SCRIPT_DIR}\n")

    # Use "today" for both odds & props
    today = date.today()
    today_str = today.strftime("%Y-%m-%d")
    logging.info(f"Betting ingest date: {today_str}")

    # 1) Odds – for today only
    run_script(
        "odds_ingest.py",
        ["--start-date", today_str, "--end-date", today_str],
    )

    # 2) Player props – for today only
    run_script(
        "props_ingest.py",
        ["--start-date", today_str, "--end-date", today_str],
    )

    logging.info("\n✅ Betting ingest finished for today.")


if __name__ == "__main__":
    main()
