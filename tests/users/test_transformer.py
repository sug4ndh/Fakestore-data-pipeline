"""Tests for users/transformer.py"""

import os
from datetime import datetime
import pandas as pd
from pathlib import Path
import pytest
import sys

for _mod in ["fetcher", "storer", "transformer"]:
    sys.modules.pop(_mod, None)

sys.path.insert(0, str(Path(__file__).parents[2] / "pipelines" / "users"))
sys.path.insert(0, str(Path(__file__).parents[2]))

os.environ.setdefault("PIPELINE_SECRET_KEY", "test-secret-key-for-unit-tests")

from shared.schemas.users import UserModel
from storer import store_users
from transformer import _apply_silver_transforms, bronze_to_silver, silver_to_gold


@pytest.fixture
def bronze_df(sample_users_raw):
    df = pd.json_normalize(sample_users_raw, sep="_")
    df["_ingested_at"] = datetime(2026, 3, 19, 10, 0, 0)
    return df


@pytest.fixture
def populated_bronze(sample_users_raw, run_date, tmp_path):
    users = [UserModel.model_validate(u) for u in sample_users_raw]
    store_users(users, run_date, data_root=tmp_path)
    return tmp_path


def test_pii_fields_masked_to_64_chars(bronze_df):
    """All PII fields are replaced with 64-char hex strings."""
    result = _apply_silver_transforms(bronze_df)
    for col in ["email", "username", "name_firstname", "name_lastname", "phone"]:
        assert result[col].apply(lambda v: len(v) == 64).all()


def test_pii_masking_is_deterministic(bronze_df):
    """Same input always produces the same hash."""
    r1 = _apply_silver_transforms(bronze_df)
    r2 = _apply_silver_transforms(bronze_df)
    assert r1["email"].tolist() == r2["email"].tolist()


def test_password_dropped(bronze_df):
    """password is absent from Silver."""
    result = _apply_silver_transforms(bronze_df)
    assert "password" not in result.columns


def test_silver_deduplication(populated_bronze, run_date):
    """Silver has one row per unique id."""
    silver_df = bronze_to_silver(run_date, data_root=populated_bronze)
    assert silver_df["id"].nunique() == len(silver_df)


def test_incremental_run(sample_users_raw, prev_run_date, run_date, tmp_path):
    """Two sequential runs produce a deduplicated Silver snapshot."""
    users = [UserModel.model_validate(u) for u in sample_users_raw]

    store_users(users, prev_run_date, data_root=tmp_path)
    bronze_to_silver(prev_run_date, data_root=tmp_path)

    store_users(users, run_date, data_root=tmp_path)
    silver_df = bronze_to_silver(run_date, data_root=tmp_path)

    assert len(silver_df) == len(sample_users_raw)
    assert silver_df["id"].nunique() == len(sample_users_raw)


def test_pandera_raises_if_password_present(populated_bronze, run_date, monkeypatch):
    """Pipeline fails if password column survives into Silver."""
    import transformer as t
    original = t._apply_silver_transforms

    def inject_password(df):
        result = original(df)
        result["password"] = "oops"
        return result

    monkeypatch.setattr(t, "_apply_silver_transforms", inject_password)
    with pytest.raises(ValueError, match="password column must be dropped"):
        bronze_to_silver(run_date, data_root=populated_bronze)


def test_gold_ids_are_positive(populated_bronze, run_date):
    """Gold partition is written and all ids are positive integers."""
    bronze_to_silver(run_date, data_root=populated_bronze)
    gold_df = silver_to_gold(run_date, data_root=populated_bronze)
    assert (gold_df["id"] > 0).all()
