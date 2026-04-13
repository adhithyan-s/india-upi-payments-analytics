"""
Unit tests for the UPI transaction generator.

These tests run in GitHub Actions CI/CD on every push. 
A failing test blocks the merge — data quality starts at generation, not just in the warehouse.

Run with: make test
"""

import sys
import os
import numpy as np
import pandas as pd
import pytest
from datetime import date

# Add generator/ to path so we can import from it
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "generator"))

from generator.config import (
    CITIES,
    MERCHANT_CATEGORIES,
    UPI_APPS,
    PAYMENT_TYPES,
    FESTIVAL_DATES,
    HOUR_WEIGHTS,
    get_city_weights,
)
from generator.generate_transactions import (
    build_object_key,
    build_date_range,
    df_to_parquet_bytes,
    generate_batch,
)


# ----------------------------------------------------------
# Fixtures — reusable test setup
# ----------------------------------------------------------

@pytest.fixture
def rng():
    """Reproducible random generator for tests."""
    return np.random.default_rng(seed=0)


@pytest.fixture
def generation_params():
    """Pre-computed weights and lookups for generate_batch."""
    city_names = [c[0] for c in CITIES]
    raw_city_weights = np.array(get_city_weights(CITIES), dtype=float)
    city_weights_norm = raw_city_weights / raw_city_weights.sum()

    merchant_codes = [m["category_code"] for m in MERCHANT_CATEGORIES]
    raw_merchant_weights = np.array(
        [m["weight"] for m in MERCHANT_CATEGORIES], dtype=float
    )
    merchant_weights = raw_merchant_weights / raw_merchant_weights.sum()
    merchant_lookup = {m["category_code"]: m for m in MERCHANT_CATEGORIES}

    payment_codes = [p[0] for p in PAYMENT_TYPES]
    raw_payment_weights = np.array([p[2] for p in PAYMENT_TYPES], dtype=float)
    payment_weights = raw_payment_weights / raw_payment_weights.sum()

    app_names = [a[0] for a in UPI_APPS]
    raw_app_weights = np.array([a[3] for a in UPI_APPS], dtype=float)
    app_weights = raw_app_weights / raw_app_weights.sum()

    from generator.config import STATUSES, FAILURE_REASONS, DEVICE_TYPES
    status_values = [s[0] for s in STATUSES]
    status_probs = np.array([s[1] for s in STATUSES])
    failure_reasons = [f[0] for f in FAILURE_REASONS]
    failure_probs = np.array([f[1] for f in FAILURE_REASONS])
    failure_probs = failure_probs / failure_probs.sum()
    device_types = [d[0] for d in DEVICE_TYPES]
    device_probs = np.array([d[1] for d in DEVICE_TYPES])

    hour_weights_arr = np.array(HOUR_WEIGHTS, dtype=float)
    hour_weights_norm = hour_weights_arr / hour_weights_arr.sum()

    return dict(
        city_names=city_names,
        city_weights_norm=city_weights_norm,
        merchant_codes=merchant_codes,
        merchant_weights=merchant_weights,
        merchant_lookup=merchant_lookup,
        payment_codes=payment_codes,
        payment_weights=payment_weights,
        app_names=app_names,
        app_weights=app_weights,
        status_values=status_values,
        status_probs=status_probs,
        failure_reasons=failure_reasons,
        failure_probs=failure_probs,
        device_types=device_types,
        device_probs=device_probs,
        hour_weights_norm=hour_weights_norm,
    )


@pytest.fixture
def sample_df(rng, generation_params):
    """A small generated DataFrame for testing."""
    return generate_batch(
        rng=rng,
        batch_size=1000,
        date=date(2024, 1, 15),
        **generation_params,
    )


# ----------------------------------------------------------
# Config tests
# ----------------------------------------------------------

class TestConfig:

    def test_cities_not_empty(self):
        assert len(CITIES) > 0

    def test_all_cities_have_required_fields(self):
        for city in CITIES:
            assert len(city) == 5, f"City tuple should have 5 fields: {city}"
            name, state, region, tier, is_metro = city
            assert isinstance(name, str) and len(name) > 0
            assert tier in ("Tier 1", "Tier 2", "Tier 3")
            assert region in ("North", "South", "East", "West")
            assert isinstance(is_metro, bool)

    def test_tier1_metros_are_marked(self):
        metros = [c for c in CITIES if c[4] is True]
        assert len(metros) == 8, "Should have exactly 8 metro cities"

    def test_merchant_categories_have_required_fields(self):
        required = [
            "category_code", "category_name", "category_group",
            "avg_amount_inr", "std_amount_inr", "min_amount",
            "max_amount", "weight", "failure_rate",
        ]
        for cat in MERCHANT_CATEGORIES:
            for field in required:
                assert field in cat, f"Missing field '{field}' in {cat}"

    def test_merchant_amounts_are_valid(self):
        for cat in MERCHANT_CATEGORIES:
            assert cat["min_amount"] > 0
            assert cat["max_amount"] > cat["min_amount"]
            assert cat["avg_amount_inr"] > 0
            assert 0 < cat["failure_rate"] < 1

    def test_upi_app_weights_sum_to_100(self):
        total = sum(a[3] for a in UPI_APPS)
        assert total == 100, f"UPI app weights should sum to 100, got {total}"

    def test_hour_weights_length(self):
        assert len(HOUR_WEIGHTS) == 24

    def test_city_weights_returns_correct_length(self):
        weights = get_city_weights(CITIES)
        assert len(weights) == len(CITIES)

    def test_festival_dates_format(self):
        for date_str in FESTIVAL_DATES.keys():
            # Should parse without error
            from datetime import datetime
            parsed = datetime.strptime(date_str, "%Y-%m-%d")
            assert parsed is not None


# ----------------------------------------------------------
# Object key / partitioning tests
# ----------------------------------------------------------

class TestPartitioning:

    def test_object_key_format(self):
        key = build_object_key(2024, 1, "Mumbai", 0)
        assert key == "year=2024/month=01/city=Mumbai/batch_0000.parquet"

    def test_object_key_pads_month(self):
        """Month must be zero-padded to 2 digits for correct sort order."""
        key = build_object_key(2024, 3, "Delhi", 1)
        assert "month=03" in key

    def test_object_key_handles_city_spaces(self):
        """Cities with spaces must have spaces replaced for valid S3 keys."""
        key = build_object_key(2024, 1, "Navi Mumbai", 0)
        assert " " not in key
        assert "Navi_Mumbai" in key

    def test_object_key_hive_format(self):
        """
        🎯 INTERVIEW POINT: Hive partition format uses key=value.
        Spark, Athena, and dbt read this automatically for partition pruning.
        """
        key = build_object_key(2024, 6, "Bengaluru", 5)
        assert "year=" in key
        assert "month=" in key
        assert "city=" in key
        assert key.endswith(".parquet")

    def test_date_range_length(self):
        dates = build_date_range("2024-01-01", "2024-01-31")
        assert len(dates) == 31

    def test_date_range_inclusive(self):
        dates = build_date_range("2024-01-01", "2024-01-03")
        assert str(dates[0]) == "2024-01-01"
        assert str(dates[-1]) == "2024-01-03"


# ----------------------------------------------------------
# Generated DataFrame tests
# ----------------------------------------------------------

class TestGeneratedData:

    def test_correct_row_count(self, sample_df):
        assert len(sample_df) == 1000

    def test_required_columns_present(self, sample_df):
        required_cols = [
            "txn_id", "amount_inr", "city", "merchant_category",
            "upi_app", "payment_type", "status", "failure_reason",
            "txn_date", "txn_hour", "is_weekend", "is_festival",
            "sender_upi", "receiver_upi", "device_type", "created_at",
        ]
        for col in required_cols:
            assert col in sample_df.columns, f"Missing column: {col}"

    def test_no_null_txn_ids(self, sample_df):
        assert sample_df["txn_id"].isna().sum() == 0

    def test_txn_ids_are_unique(self, sample_df):
        """Unique txn_ids are critical — duplicates would corrupt deduplication logic in the Silver layer."""
        assert sample_df["txn_id"].nunique() == len(sample_df)

    def test_txn_ids_have_correct_prefix(self, sample_df):
        assert sample_df["txn_id"].str.startswith("TXN").all()

    def test_amounts_are_positive(self, sample_df):
        """
        This is also tested as a dbt singular test (assert_no_negative_amounts.sql). 
        We test it here too because catching it at generation is cheaper than
        catching it after loading 50M rows.
        """
        assert (sample_df["amount_inr"] > 0).all()

    def test_amounts_are_numeric(self, sample_df):
        assert pd.api.types.is_float_dtype(sample_df["amount_inr"])

    def test_status_values_valid(self, sample_df):
        valid = {"SUCCESS", "FAILED", "PENDING"}
        actual = set(sample_df["status"].unique())
        assert actual.issubset(valid), f"Invalid statuses: {actual - valid}"

    def test_failure_reason_null_for_success(self, sample_df):
        """Failed transactions must have a reason. Successful ones must not."""
        success_rows = sample_df[sample_df["status"] == "SUCCESS"]
        assert success_rows["failure_reason"].isna().all()

    def test_failure_reason_not_null_for_failed(self, sample_df):
        failed_rows = sample_df[sample_df["status"] == "FAILED"]
        if len(failed_rows) > 0:
            assert failed_rows["failure_reason"].notna().all()

    def test_hours_in_valid_range(self, sample_df):
        assert sample_df["txn_hour"].between(0, 23).all()

    def test_cities_from_config(self, sample_df):
        config_cities = {c[0] for c in CITIES}
        generated_cities = set(sample_df["city"].unique())
        assert generated_cities.issubset(config_cities)

    def test_upi_apps_from_config(self, sample_df):
        config_apps = {a[0] for a in UPI_APPS}
        generated_apps = set(sample_df["upi_app"].unique())
        assert generated_apps.issubset(config_apps)

    def test_sender_upi_format(self, sample_df):
        """UPI IDs must contain @ separator."""
        assert sample_df["sender_upi"].str.contains("@").all()

    def test_receiver_upi_format(self, sample_df):
        assert sample_df["receiver_upi"].str.contains("@").all()

    def test_is_weekend_correct(self, rng, generation_params):
        """Monday 2024-01-15 should have is_weekend=False."""
        df = generate_batch(
            rng=rng, batch_size=100,
            date=date(2024, 1, 15),  # Monday
            **generation_params
        )
        assert not df["is_weekend"].any()

    def test_is_weekend_saturday(self, rng, generation_params):
        """Saturday should have is_weekend=True."""
        df = generate_batch(
            rng=rng, batch_size=100,
            date=date(2024, 1, 13),  # Saturday
            **generation_params
        )
        assert df["is_weekend"].all()

    def test_festival_flag_on_diwali(self, rng, generation_params):
        """Diwali 2024 (Oct 31) should have is_festival=True."""
        df = generate_batch(
            rng=rng, batch_size=100,
            date=date(2024, 10, 31),
            **generation_params
        )
        assert df["is_festival"].all()

    def test_festival_flag_on_normal_day(self, rng, generation_params):
        """A random non-festival day should have is_festival=False."""
        df = generate_batch(
            rng=rng, batch_size=100,
            date=date(2024, 3, 5),   # no festival
            **generation_params
        )
        assert not df["is_festival"].any()


# ----------------------------------------------------------
# Parquet serialisation tests
# ----------------------------------------------------------

class TestParquetSerialisation:

    def test_parquet_bytes_not_empty(self, sample_df):
        result = df_to_parquet_bytes(sample_df)
        assert len(result) > 0

    def test_parquet_bytes_readable(self, sample_df):
        """
        round-trip test — serialise to Parquet bytes then read back. 
        Verifies the bytes are valid Parquet, not just non-empty. 
        Row count and column count must match.
        """
        import io
        parquet_bytes = df_to_parquet_bytes(sample_df)
        recovered = pd.read_parquet(io.BytesIO(parquet_bytes))
        assert len(recovered) == len(sample_df)
        assert set(recovered.columns) == set(sample_df.columns)

    def test_parquet_compression_reduces_size(self, sample_df):
        """Parquet with Snappy should be smaller than raw CSV bytes."""
        parquet_bytes = df_to_parquet_bytes(sample_df)
        csv_bytes = sample_df.to_csv(index=False).encode()
        assert len(parquet_bytes) < len(csv_bytes), (
            f"Parquet ({len(parquet_bytes):,}B) should be smaller "
            f"than CSV ({len(csv_bytes):,}B)"
        )