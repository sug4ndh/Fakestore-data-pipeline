"""Tests for carts/transformer.py"""

import sys
from pathlib import Path

import pytest

for _mod in ["fetcher", "storer", "transformer"]:
    sys.modules.pop(_mod, None)

sys.path.insert(0, str(Path(__file__).parents[2] / "pipelines" / "carts"))
sys.path.insert(0, str(Path(__file__).parents[2]))

from shared.schemas.carts import CartModel
from storer import store_carts
from transformer import bronze_to_silver, silver_to_gold


@pytest.fixture
def populated_bronze(sample_carts_raw, run_date, tmp_path):
    carts = [CartModel.model_validate(c) for c in sample_carts_raw]
    store_carts(carts, run_date, data_root=tmp_path)
    return tmp_path


def test_silver_deduplication(populated_bronze, run_date):
    """Silver has no duplicate (id, productId) pairs."""
    silver_df = bronze_to_silver(run_date, data_root=populated_bronze)
    dedup = silver_df[["id", "productId"]].drop_duplicates()
    assert len(dedup) == len(silver_df)


def test_incremental_run(sample_carts_raw, prev_run_date, run_date, tmp_path):
    """Two sequential runs produce a deduplicated Silver snapshot."""
    carts = [CartModel.model_validate(c) for c in sample_carts_raw]
    expected = sum(len(c.products) for c in carts)

    store_carts(carts, prev_run_date, data_root=tmp_path)
    bronze_to_silver(prev_run_date, data_root=tmp_path)

    store_carts(carts, run_date, data_root=tmp_path)
    silver_df = bronze_to_silver(run_date, data_root=tmp_path)

    assert len(silver_df) == expected
    assert len(silver_df[["id", "productId"]].drop_duplicates()) == expected


def test_gold_quantity_is_positive(populated_bronze, run_date):
    """Gold enforces quantity > 0."""
    bronze_to_silver(run_date, data_root=populated_bronze)
    gold_df = silver_to_gold(run_date, data_root=populated_bronze)
    assert (gold_df["quantity"] > 0).all()


def test_gold_pandera_raises_on_zero_quantity(populated_bronze, run_date):
    """Pandera raises when quantity is zero."""
    from shared.schemas.carts import gold_carts_schema
    import pandera

    bronze_to_silver(run_date, data_root=populated_bronze)
    gold_df = silver_to_gold(run_date, data_root=populated_bronze)

    bad_df = gold_df.copy()
    bad_df["quantity"] = 0

    with pytest.raises(pandera.errors.SchemaError):
        gold_carts_schema.validate(bad_df)
