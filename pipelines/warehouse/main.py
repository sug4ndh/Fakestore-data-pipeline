"""Builds warehouse.duckdb with all 12 tables across all layers and endpoints.

Runs after all three pipeline containers complete (using docker-compose.yml depends_on).
Raw and Bronze tables glob all partitions to expose full history.
Silver and Gold tables read the latest partition only. each is already a
deduplicated snapshot so there's no need to read the full history.
"""

import sys
from pathlib import Path

import duckdb

from shared.utils import get_data_root, get_logger

logger = get_logger("warehouse")

ENDPOINTS = ["users", "products", "carts"]


def build_warehouse(data_root: Path | None = None) -> None:
    if data_root is None:
        data_root = get_data_root()

    db_path = data_root / "warehouse.duckdb"
    logger.info(f"Building warehouse at {db_path}")

    con = duckdb.connect(str(db_path))

    try:
        for endpoint in ENDPOINTS:
            _register_bronze(con, data_root, endpoint)
            _register_silver(con, data_root, endpoint)
            _register_gold(con, data_root, endpoint)

        _log_table_summary(con)

    finally:
        con.close()

    logger.info("Warehouse build complete")


def _register_bronze(con: duckdb.DuckDBPyConnection, data_root: Path, endpoint: str) -> None:
    """Register all Bronze Parquet partitions as a single table.

    hive_partitioning=true automatically injects ingestion_date as a queryable column.
    """
    glob = str(data_root / "bronze" / endpoint / "**" / "*.parquet")
    table = f"bronze_{endpoint}"

    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE {table} AS
            SELECT * FROM read_parquet('{glob}', hive_partitioning=true)
        """)
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        logger.info(f"  {table}: {count} rows")
    except Exception as exc:
        logger.warning(f"  {table}: skipped ({exc})")


def _register_silver(con: duckdb.DuckDBPyConnection, data_root: Path, endpoint: str) -> None:
    """Register the latest Silver partition as a table."""
    latest = _get_latest_parquet(data_root, "silver", endpoint)
    table = f"silver_{endpoint}"

    if latest is None:
        logger.warning(f"  {table}: no Silver partition found, skipping")
        return

    con.execute(f"""
        CREATE OR REPLACE TABLE {table} AS
        SELECT * FROM read_parquet('{latest}')
    """)
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    logger.info(f"  {table}: {count} rows (from {Path(latest).parent.name})")


def _register_gold(con: duckdb.DuckDBPyConnection, data_root: Path, endpoint: str) -> None:
    """Register the latest Gold partition as a table."""
    latest = _get_latest_parquet(data_root, "gold", endpoint)
    table = f"gold_{endpoint}"

    if latest is None:
        logger.warning(f"  {table}: no Gold partition found, skipping")
        return

    con.execute(f"""
        CREATE OR REPLACE TABLE {table} AS
        SELECT * FROM read_parquet('{latest}')
    """)
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    logger.info(f"  {table}: {count} rows (from {Path(latest).parent.name})")


def _get_latest_parquet(data_root: Path, layer: str, endpoint: str) -> str | None:
    """Return the path of the most recent Parquet file for a layer/endpoint."""
    base = data_root / layer / endpoint
    if not base.exists():
        return None
    files = sorted(base.glob("**/*.parquet"))
    return str(files[-1]) if files else None


def _log_table_summary(con: duckdb.DuckDBPyConnection) -> None:
    tables = con.execute("SHOW TABLES").fetchall()
    logger.info(f"Warehouse tables registered: {[t[0] for t in tables]}")


if __name__ == "__main__":
    try:
        build_warehouse()
    except Exception:
        logger.exception("Warehouse build failed")
        sys.exit(1)
