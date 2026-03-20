import hashlib
import hmac
import logging
import os
from pathlib import Path

import pandas as pd


def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    )
    return logging.getLogger(name)


def get_data_root() -> Path:
    return Path(os.environ.get("DATA_ROOT", "/data"))


def pseudonymize(value: str) -> str:
    """Returns a deterministic 64-char hex digest of the given value.

    Requires PIPELINE_SECRET_KEY to be set in the environment.
    """
    secret = os.environ["PIPELINE_SECRET_KEY"].encode()
    return hmac.new(secret, str(value).encode(), hashlib.sha256).hexdigest()


def get_partition_path(data_root: Path, layer: str, endpoint: str, run_date: str) -> Path:
    """Returns the partition directory for a given layer, endpoint, and date.

    Example: data_root/bronze/users/ingestion_date=2026-03-19/
    """
    return data_root / layer / endpoint / f"ingestion_date={run_date}"


def get_latest_partition_before(
    data_root: Path,
    layer: str,
    endpoint: str,
    run_date: str,
) -> pd.DataFrame | None:
    """Returns the DataFrame from the most recent partition strictly before run_date.

    Returns None if no prior partition exists (first run).
    """
    base_path = data_root / layer / endpoint
    if not base_path.exists():
        return None

    prior_dates = [
        p.name.replace("ingestion_date=", "")
        for p in base_path.iterdir()
        if p.is_dir()
        and p.name.startswith("ingestion_date=")
        and p.name.replace("ingestion_date=", "") < run_date
    ]

    if not prior_dates:
        return None

    latest_date = max(prior_dates)
    parquet_files = list((base_path / f"ingestion_date={latest_date}").glob("*.parquet"))
    if not parquet_files:
        return None

    return pd.read_parquet(parquet_files[0])
