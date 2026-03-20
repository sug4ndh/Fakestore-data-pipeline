"""Products pipeline entrypoint"""

import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from fetcher import fetch_products
from storer import store_products
from transformer import run_transformations

from shared.utils import get_logger

logger = get_logger("products.pipeline")


def main() -> None:
    run_date = os.environ.get("RUN_DATE") or date.today().isoformat()
    logger.info(f"=== Products pipeline starting | run_date={run_date} ===")

    try:
        products = fetch_products()
        logger.info(f"Fetched {len(products)} products")

        store_products(products, run_date)
        run_transformations(run_date)

    except Exception:
        logger.exception("Products pipeline failed")
        sys.exit(1)

    logger.info("=== Products pipeline completed successfully ===")


if __name__ == "__main__":
    main()
