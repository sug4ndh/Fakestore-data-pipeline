"""Bronze to Silver to Gold transformations for the carts endpoint."""

from pathlib import Path

import pandas as pd

from shared.schemas.carts import gold_carts_schema, silver_carts_schema
from shared.utils import (
    get_data_root,
    get_latest_partition_before,
    get_logger,
    get_partition_path,
)

logger = get_logger(__name__)

ENDPOINT = "carts"


def run_transformations(run_date: str, data_root: Path | None = None) -> None:
    """Run Bronze to Silver to Gold for the given run_date."""
    if data_root is None:
        data_root = get_data_root()
    bronze_to_silver(run_date, data_root)
    silver_to_gold(run_date, data_root)


def bronze_to_silver(run_date: str, data_root: Path | None = None) -> pd.DataFrame:
    """Build a deduplicated Silver snapshot for run_date.

    Merges today's Bronze partition with the previous Silver snapshot,
    then deduplicates by (id, productId) keeping the latest record.
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
        logger.info("No previous Silver snapshot. First run")
        combined = df

    # Deduping by (id, productId)
    silver_df = (
        combined
        .sort_values("_ingested_at", ascending=False)
        .drop_duplicates(subset=["id", "productId"])
        .reset_index(drop=True)
    )
    logger.info(f"After deduplication: {len(silver_df)} rows")

    silver_carts_schema.validate(silver_df)
    logger.info("Silver Pandera validation passed")

    silver_path = get_partition_path(data_root, "silver", ENDPOINT, run_date)
    silver_path.mkdir(parents=True, exist_ok=True)
    silver_df.to_parquet(silver_path / f"{ENDPOINT}.parquet", index=False)
    logger.info(f"Silver Parquet saved at {silver_path} ({len(silver_df)} rows)")

    return silver_df


def silver_to_gold(run_date: str, data_root: Path | None = None) -> pd.DataFrame:
    """Apply final type casts and validate then write Gold."""
    if data_root is None:
        data_root = get_data_root()

    silver_path = get_partition_path(data_root, "silver", ENDPOINT, run_date)
    silver_df = pd.read_parquet(silver_path / f"{ENDPOINT}.parquet")
    logger.info(f"Silver read: {len(silver_df)} rows for {run_date}")

    gold_df = _apply_gold_transforms(silver_df)

    gold_carts_schema.validate(gold_df)
    logger.info("Gold Pandera validation passed")

    gold_path = get_partition_path(data_root, "gold", ENDPOINT, run_date)
    gold_path.mkdir(parents=True, exist_ok=True)
    gold_df.to_parquet(gold_path / f"{ENDPOINT}.parquet", index=False)
    logger.info(f"Gold Parquet saved at {gold_path} ({len(gold_df)} rows)")

    return gold_df


def _apply_silver_transforms(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["id"] = pd.to_numeric(df["id"], errors="coerce").astype(int)
    df["userId"] = pd.to_numeric(df["userId"], errors="coerce").astype(int)
    df["productId"] = pd.to_numeric(df["productId"], errors="coerce").astype(int)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").astype(int)
    df["date"] = pd.to_datetime(df["date"], utc=True).dt.tz_localize(None)
    return df


def _apply_gold_transforms(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["id"] = df["id"].astype(int)
    df["userId"] = df["userId"].astype(int)
    df["productId"] = df["productId"].astype(int)
    df["quantity"] = df["quantity"].astype(int)
    return df
