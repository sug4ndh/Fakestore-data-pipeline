"""Tests for products/transformer.py"""

import sys
from pathlib import Path

import pytest

for _mod in ["fetcher", "storer", "transformer"]:
    sys.modules.pop(_mod, None)

sys.path.insert(0, str(Path(__file__).parents[2] / "pipelines" / "products"))
sys.path.insert(0, str(Path(__file__).parents[2]))

from shared.schemas.products import ProductModel
from storer import store_products
from transformer import bronze_to_silver, silver_to_gold


@pytest.fixture
def populated_bronze(sample_products_raw, run_date, tmp_path):
    products = [ProductModel.model_validate(p) for p in sample_products_raw]
    store_products(products, run_date, data_root=tmp_path)
    return tmp_path


def test_silver_deduplication(populated_bronze, run_date):
    """Silver has one row per unique id."""
    silver_df = bronze_to_silver(run_date, data_root=populated_bronze)
    assert silver_df["id"].nunique() == len(silver_df)


def test_incremental_run(sample_products_raw, prev_run_date, run_date, tmp_path):
    """Two sequential runs produce a deduplicated Silver snapshot."""
    products = [ProductModel.model_validate(p) for p in sample_products_raw]

    store_products(products, prev_run_date, data_root=tmp_path)
    bronze_to_silver(prev_run_date, data_root=tmp_path)

    store_products(products, run_date, data_root=tmp_path)
    silver_df = bronze_to_silver(run_date, data_root=tmp_path)

    assert len(silver_df) == len(sample_products_raw)
    assert silver_df["id"].nunique() == len(sample_products_raw)


def test_gold_price_is_positive(populated_bronze, run_date):
    """Gold enforces price > 0."""
    bronze_to_silver(run_date, data_root=populated_bronze)
    gold_df = silver_to_gold(run_date, data_root=populated_bronze)
    assert (gold_df["price"] > 0).all()


def test_gold_pandera_raises_on_negative_price(populated_bronze, run_date):
    """Pandera raises when price is negative."""
    from shared.schemas.products import gold_products_schema
    import pandera

    bronze_to_silver(run_date, data_root=populated_bronze)
    gold_df = silver_to_gold(run_date, data_root=populated_bronze)

    bad_df = gold_df.copy()
    bad_df["price"] = -1.0

    with pytest.raises(pandera.errors.SchemaError):
        gold_products_schema.validate(bad_df)
