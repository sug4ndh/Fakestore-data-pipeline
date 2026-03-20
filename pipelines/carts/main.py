"""Carts pipeline entrypoint"""

import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from fetcher import fetch_carts
from storer import store_carts
from transformer import run_transformations

from shared.utils import get_logger

logger = get_logger("carts.pipeline")


def main() -> None:
    run_date = os.environ.get("RUN_DATE") or date.today().isoformat()
    logger.info(f"=== Carts pipeline starting | run_date={run_date} ===")

    try:
        carts = fetch_carts()
        logger.info(f"Fetched {len(carts)} carts")

        store_carts(carts, run_date)
        run_transformations(run_date)

    except Exception:
        logger.exception("Carts pipeline failed")
        sys.exit(1)

    logger.info("=== Carts pipeline completed successfully ===")


if __name__ == "__main__":
    main()
