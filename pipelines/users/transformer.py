"""Bronze to Silver to Gold transformations for the users endpoint."""

from pathlib import Path

import pandas as pd

from shared.schemas.users import gold_users_schema, silver_users_schema
from shared.utils import (
    get_data_root,
    get_latest_partition_before,
    get_logger,
    get_partition_path,
    pseudonymize,
)

logger = get_logger(__name__)

ENDPOINT = "users"

_PII_MASK_COLS = ["email", "username", "name_firstname", "name_lastname", "phone"]
_PII_DROP_COLS = [
    "password",
    "address_street",
    "address_number",
    "address_geolocation_lat",
    "address_geolocation_long",
]


def run_transformations(run_date: str, data_root: Path | None = None) -> None:
    """Run Bronze to Silver to Gold for the given run_date."""
    if data_root is None:
        data_root = get_data_root()

    bronze_to_silver(run_date, data_root)
    silver_to_gold(run_date, data_root)


def bronze_to_silver(run_date: str, data_root: Path | None = None) -> pd.DataFrame:
    """Build a deduplicated Silver snapshot for run_date.

    Reads today's Bronze partition and merges it with the previous Silver snapshot,
    then deduplicates by id keeping the latest record. Pandera validates the result
    before anything is written to disk.
    """
    if data_root is None:
        data_root = get_data_root()

    bronze_path = get_partition_path(data_root, "bronze", ENDPOINT, run_date)
    bronze_df = pd.read_parquet(bronze_path / f"{ENDPOINT}.parquet")
    logger.info(f"Bronze read: {len(bronze_df)} rows for {run_date}")

    df = _apply_silver_transforms(bronze_df)

    prev_silver = get_latest_partition_before(data_root, "silver", ENDPOINT, run_date)
    if prev_silver is not None:
        logger.info(f"Previous Silver snapshot: {len(prev_silver)} rows")
        combined = pd.concat([df, prev_silver], ignore_index=True)
    else:
        logger.info("No previous Silver snapshot — first run")
        combined = df

    # Deduping by id
    silver_df = (
        combined
        .sort_values("_ingested_at", ascending=False)
        .drop_duplicates(subset=["id"])
        .reset_index(drop=True)
    )
    logger.info(f"After deduplication: {len(silver_df)} rows")

    _assert_no_password(silver_df)
    silver_users_schema.validate(silver_df)
    logger.info("Silver Pandera validation passed")

    silver_path = get_partition_path(data_root, "silver", ENDPOINT, run_date)
    silver_path.mkdir(parents=True, exist_ok=True)
    silver_df.to_parquet(silver_path / f"{ENDPOINT}.parquet", index=False)
    logger.info(f"Silver Parquet saved at {silver_path} ({len(silver_df)} rows)")

    return silver_df


def silver_to_gold(run_date: str, data_root: Path | None = None) -> pd.DataFrame:
    """Apply final type casts and validate then write Gold.

    Reads Silver for run_date which is already a complete deduplicated snapshot
    so no merge with a previous Gold partition is needed.
    """
    if data_root is None:
        data_root = get_data_root()

    silver_path = get_partition_path(data_root, "silver", ENDPOINT, run_date)
    silver_df = pd.read_parquet(silver_path / f"{ENDPOINT}.parquet")
    logger.info(f"Silver read: {len(silver_df)} rows for {run_date}")

    gold_df = _apply_gold_transforms(silver_df)

    gold_users_schema.validate(gold_df)
    logger.info("Gold Pandera validation passed")

    gold_path = get_partition_path(data_root, "gold", ENDPOINT, run_date)
    gold_path.mkdir(parents=True, exist_ok=True)
    gold_df.to_parquet(gold_path / f"{ENDPOINT}.parquet", index=False)
    logger.info(f"Gold Parquet saved at {gold_path} ({len(gold_df)} rows)")

    return gold_df


def _apply_silver_transforms(df: pd.DataFrame) -> pd.DataFrame:
    """Mask PII fields in-place and drop sensitive columns."""
    df = df.copy()

    for col in _PII_MASK_COLS:
        if col in df.columns:
            df[col] = df[col].apply(pseudonymize)

    cols_to_drop = [c for c in _PII_DROP_COLS if c in df.columns]
    df = df.drop(columns=cols_to_drop)

    return df


def _apply_gold_transforms(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["id"] = df["id"].astype(int)
    if "address_zipcode" in df.columns:
        df["address_zipcode"] = df["address_zipcode"].astype(str)
    return df


def _assert_no_password(df: pd.DataFrame) -> None:
    """Raise if the password column is still present — it must be dropped at Silver."""
    if "password" in df.columns:
        raise ValueError("password column must be dropped at Silver.")
