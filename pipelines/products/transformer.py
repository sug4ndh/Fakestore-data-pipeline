"""Bronze to Silver to Gold transformations for the products endpoint."""

from pathlib import Path

import pandas as pd

from shared.schemas.products import gold_products_schema, silver_products_schema
from shared.utils import (
    get_data_root,
    get_latest_partition_before,
    get_logger,
    get_partition_path,
)

logger = get_logger(__name__)

ENDPOINT = "products"


def run_transformations(run_date: str, data_root: Path | None = None) -> None:
    """Run Bronze to Silver to Gold for the given run_date."""
    if data_root is None:
        data_root = get_data_root()
    bronze_to_silver(run_date, data_root)
    silver_to_gold(run_date, data_root)


def bronze_to_silver(run_date: str, data_root: Path | None = None) -> pd.DataFrame:
    """Build a deduplicated Silver snapshot for run_date.

    Merges today's Bronze partition with the previous Silver snapshot,
    then deduplicates by id keeping the latest record.
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

    silver_products_schema.validate(silver_df)
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

    gold_products_schema.validate(gold_df)
    logger.info("Gold Pandera validation passed")

    gold_path = get_partition_path(data_root, "gold", ENDPOINT, run_date)
    gold_path.mkdir(parents=True, exist_ok=True)
    gold_df.to_parquet(gold_path / f"{ENDPOINT}.parquet", index=False)
    logger.info(f"Gold Parquet saved at {gold_path} ({len(gold_df)} rows)")

    return gold_df


def _apply_silver_transforms(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["id"] = df["id"].astype(int)
    df["price"] = df["price"].astype(float)
    if "rating_rate" in df.columns:
        df["rating_rate"] = pd.to_numeric(df["rating_rate"], errors="coerce")
    if "rating_count" in df.columns:
        df["rating_count"] = pd.to_numeric(df["rating_count"], errors="coerce").astype("Int64")
    return df


def _apply_gold_transforms(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["id"] = df["id"].astype(int)
    df["price"] = df["price"].astype(float)
    return df
