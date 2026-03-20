"""
Users pipeline entrypoint

Reads RUN_DATE from the environment (defaults to today) and runs
fetcher -> storer -> transformer in sequence.

In production, Airflow would inject RUN_DATE={{ logical_date }}.
For local backfill: docker compose run users -e RUN_DATE=2026-03-19
"""

import os
import sys
from datetime import date
from pathlib import Path

# Allow direct imports of sibling modules (fetcher, storer, transformer)
sys.path.insert(0, str(Path(__file__).parent))

from fetcher import fetch_users
from storer import store_users
from transformer import run_transformations

from shared.utils import get_logger

logger = get_logger("users.pipeline")


def main() -> None:
    run_date = os.environ.get("RUN_DATE") or date.today().isoformat()
    logger.info(f"=== Users pipeline starting | run_date={run_date} ===")

    try:
        users = fetch_users()
        logger.info(f"Fetched {len(users)} users")

        store_users(users, run_date)
        run_transformations(run_date)

    except Exception:
        logger.exception("Users pipeline failed")
        sys.exit(1)

    logger.info("=== Users pipeline completed successfully ===")


if __name__ == "__main__":
    main()
