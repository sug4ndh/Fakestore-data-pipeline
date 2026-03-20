"""Persists users data to Raw (JSON) and Bronze (Parquet)."""

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from shared.schemas.users import UserModel
from shared.utils import get_data_root, get_logger, get_partition_path

logger = get_logger(__name__)

ENDPOINT = "users"


def store_users(
    users: list[UserModel],
    run_date: str,
    data_root: Path | None = None,
) -> None:
    """Save raw JSON and Bronze Parquet for the given run_date."""
    if data_root is None:
        data_root = get_data_root()

    raw_dicts = [u.model_dump() for u in users]

    _save_raw(raw_dicts, run_date, data_root)
    _save_bronze(raw_dicts, run_date, data_root)


def _save_raw(raw_dicts: list[dict], run_date: str, data_root: Path) -> None:
    """Write the exact API response to disk as JSON untouched."""
    partition = get_partition_path(data_root, "raw", ENDPOINT, run_date)
    partition.mkdir(parents=True, exist_ok=True)
    output_path = partition / f"{ENDPOINT}.json"

    with open(output_path, "w") as f:
        json.dump(raw_dicts, f, indent=2, default=str)

    logger.info(f"Raw JSON saved at {output_path}")


def _save_bronze(raw_dicts: list[dict], run_date: str, data_root: Path) -> None:
    """Flatten nested JSON to a DataFrame and write as Parquet.

    name.firstname : name_firstname
    name.lastname : name_lastname
    address.city : address_city
    ....
    """
    ingested_at = datetime.utcnow()

    df = pd.json_normalize(raw_dicts, sep="_")
    df["_ingested_at"] = ingested_at

    partition = get_partition_path(data_root, "bronze", ENDPOINT, run_date)
    partition.mkdir(parents=True, exist_ok=True)
    output_path = partition / f"{ENDPOINT}.parquet"

    df.to_parquet(output_path, index=False)
    logger.info(f"Bronze Parquet saved at {output_path} ({len(df)} rows)")
