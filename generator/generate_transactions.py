"""
Generates synthetic UPI transaction data and writes it to MinIO as partitioned Parquet files (Bronze layer).

Flow:
    Python (this script)
        -> generates batches of UPI transactions as DataFrames
        -> serialises each batch to Parquet (Snappy compressed)
        -> uploads to MinIO: upi-lake/year=YYYY/month=MM/city=CCC/batch_NNN.parquet

Usage:
    python generator/generate_transactions.py
    (or via: make generate)
"""

import io
import os
import uuid
import logging
import warnings
from datetime import datetime, timedelta, timezone

import boto3
import numpy as np
import pandas as pd
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from config import (
    CITIES,
    FESTIVAL_DATES,
    FESTIVAL_MULTIPLIER,
    HOUR_WEIGHTS,
    MERCHANT_CATEGORIES,
    PAYMENT_TYPES,
    STATUSES,
    FAILURE_REASONS,
    DEVICE_TYPES,
    UPI_APPS,
    WEEKEND_MULTIPLIER,
    get_city_weights,
)

# Suppress pandas future warnings in output
warnings.filterwarnings("ignore", category=FutureWarning)

# ----------------------------------------------------------
# Logging setup
# ----------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()


# ----------------------------------------------------------
# Configuration from environment variables
# ----------------------------------------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "upi-lake")
NUM_TRANSACTIONS = int(os.getenv("NUM_TRANSACTIONS", "1000000"))
START_DATE = os.getenv("START_DATE", "2023-01-01")
END_DATE = os.getenv("END_DATE", "2024-12-31")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20000"))


def get_s3_client():
    """
    Returns a boto3 S3 client configured for MinIO.

    This is identical boto3 code that works against AWS S3 in production.
    The only difference is endpoint_url.
    In production, set MINIO_ENDPOINT=https://s3.amazonaws.com and
    update credentials — zero code changes required.
    """
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",       # MinIO ignores this, S3 needs it
    )


def ensure_bucket_exists(s3_client, bucket_name):
    """
    Creates the bucket if it doesn't already exist.
    Idempotent — safe to call on every run.
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        log.info(f"Bucket '{bucket_name}' already exists.")
    except ClientError:
        s3_client.create_bucket(Bucket=bucket_name)
        log.info(f"Created bucket '{bucket_name}'.")


def build_date_range(start_date_str, end_date_str):
    """
    Returns a list of all dates between start and end inclusive.
    """
    start = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    delta = (end - start).days + 1
    return [start + timedelta(days=i) for i in range(delta)]


def generate_batch(
        rng,
        batch_size,
        city_names,
        city_weights_norm,
        merchant_codes,
        merchant_weights,
        merchant_lookup,
        payment_codes,
        payment_weights,
        app_names,
        app_weights,
        status_values,
        status_probs,
        failure_reasons,
        failure_probs,
        device_types,
        device_probs,
        date,
        hour_weights_norm
):
    """
    Generates one batch of UPI transactions as a DataFrame.

    Used numpy for all random generation instead of Python's random module.
    Numpy generates entire arrays at once (vectorised) — dramatically faster than looping and calling random() for each row.
    """
    n = batch_size
    is_weekend = date.weekday() >= 5
    is_festival = date.strftime("%Y-%m-%d") in FESTIVAL_DATES

    # ----------------------------------------------------------
    # Pick hours for each transaction using hour weights
    # rng.choice with p= draws from a probability distribution — not uniform random.
    # This produces realistic time-of-day patterns (lunch/dinner peaks) instead of flat random hours.
    # ----------------------------------------------------------
    hours = rng.choice(24, size=n, p=hour_weights_norm)

    # Pick cities weighted by population/volume
    cities_chosen = rng.choice(len(city_names), size=n, p=city_weights_norm)

    # Pick merchant categories weighted by transaction frequency
    merchant_indices = rng.choice(len(merchant_codes), size=n, p=merchant_weights)

    # Pick UPI apps weighted by market share
    apps_chosen = rng.choice(len(app_names), size=n, p=app_weights)

    # Pick payment types weighted by frequency
    payment_indices = rng.choice(len(payment_codes), size=n, p=payment_weights)

    # ----------------------------------------------------------
    # Generate transaction amounts using normal distribution per merchant category
    # Each merchant category has its own mean and std deviation.
    # Food delivery amounts cluster around ₹380.
    # Travel amounts are higher and more spread out.
    # numpy.clip ensures no negative amounts and no unrealistic outliers beyond the category maximum.
    # ----------------------------------------------------------
    amounts = np.zeros(n)
    for i, merch_idx in enumerate(merchant_indices):
        cat = merchant_lookup[merchant_codes[merch_idx]]
        raw = rng.normal(cat["avg_amount_inr"], cat["std_amount_inr"])
        amounts[i] = np.clip(raw, cat["min_amount"], cat["max_amount"])
    amounts = np.round(amounts, 2)

    # ----------------------------------------------------------
    # Generate statuses — per-category failure rates
    # Categories like government services have higher failure rates
    # ----------------------------------------------------------
    statuses = []
    for merch_idx in merchant_indices:
        cat = merchant_lookup[merchant_codes[merch_idx]]
        fail_rate = cat["failure_rate"]
        # slightly higher failure on festival days (server load)
        if is_festival:
            fail_rate = min(fail_rate*1.3, 0.15)
        r = rng.random()
        if r < fail_rate:
            statuses.append("FAILED")
        elif r < fail_rate+0.01:
            statuses.append("PENDING")
        else:
            statuses.append("SUCCESS")

    # Failure reasons (only for FAILED rows)
    failure_reason_list = []
    for status in statuses:
        if status == "FAILED":
            reason = rng.choice(failure_reasons, p=failure_probs)
            failure_reason_list.append(reason)
        else:
            failure_reason_list.append(None)

    # Device types
    devices = rng.choice(device_types, size=n, p=device_probs)

    # ----------------------------------------------------------
    # Build timestamps
    # All timestamps stored as UTC with timezone info (TIMESTAMPTZ in PostgreSQL).
    # Converting to IST is done in the presentation layer (Grafana panel offset or a dim_date column) — never in the raw data itself.
    # Storing raw UTC prevents timezone bugs when querying across systems.
    # ----------------------------------------------------------
    minutes = rng.integers(0, 60, size=n)
    seconds = rng.integers(0, 60, size=n)

    timestamps = [
        datetime(
            date.year, date.month, date.day,
            int(h), int(m), int(s),
            tzinfo=timezone.utc,
        )
        for h, m, s in zip(hours, minutes, seconds)
    ]

    # ----------------------------------------------------------
    # Generate UPI IDs
    # Format: username@bankcode (e.g. rahul.sharma@okicici)
    # ----------------------------------------------------------
    bank_handles = [
        "okicici", "oksbi", "okhdfc", "okaxis",
        "ybl",     "ibl",   "upi",    "paytm",
        "okhdfcbank", "okkotak",
    ]

    def make_upi_id():
        parts = [
            "".join(rng.choice(list("abcdefghijklmnopqrstuvwxyz"), size=6)),
            rng.choice(["123", "456", "789", str(rng.integers(10, 99))]),
        ]
        handle = rng.choice(bank_handles)
        return f"{''.join(parts)}@{handle}"

    sender_upis = [make_upi_id() for _ in range(n)]
    receiver_upis = [make_upi_id() for _ in range(n)]

    # ----------------------------------------------------------
    # Generate unique transaction IDs
    # UUIDs guarantee uniqueness across distributed systems.
    # We use uuid4() (random UUID) rather than uuid1() (time-based) to avoid collisions when generating data in parallel batches.
    # Prefix "TXN" makes it identifiable in logs and queries.
    # ----------------------------------------------------------
    txn_ids = [f"TXN{uuid.uuid4().hex[:16].upper()}" for _ in range(n)]

    # Build the DataFrame
    df = pd.DataFrame({
        "txn_id":            txn_ids,
        "amount_inr":        amounts,
        "city":              [city_names[i] for i in cities_chosen],
        "merchant_category": [merchant_codes[i] for i in merchant_indices],
        "upi_app":           [app_names[i] for i in apps_chosen],
        "payment_type":      [payment_codes[i] for i in payment_indices],
        "status":            statuses,
        "failure_reason":    failure_reason_list,
        "txn_date":          date,
        "txn_hour":          hours.astype(np.int8),
        "is_weekend":        is_weekend,
        "is_festival":       is_festival,
        "sender_upi":        sender_upis,
        "receiver_upi":      receiver_upis,
        "device_type":       devices,
        "created_at":        timestamps,
    })

    return df


def df_to_parquet_bytes(df):
    """
    Serialises a DataFrame to Parquet bytes using Snappy compression.

    Snappy is the standard compression codec for data lake Parquet files. It's a balanced choice:
    - Better compression ratio than uncompressed (8-10x)
    - Faster decompression than gzip (important for query speed)
    - Splittable — Spark can read row groups in parallel
    We write to an in-memory buffer (BytesIO) instead of a temp file to avoid disk I/O overhead.
    """
    buffer = io.BytesIO()
    df.to_parquet(
        buffer,
        index=False,
        compression="snappy",
        engine="pyarrow",
    )
    buffer.seek(0)
    return buffer.getvalue()


def upload_to_minio(s3_client, bucket, key, parquet_bytes):
    """
    Uploads Parquet bytes to MinIO under the given key.

    PUT to object storage is naturally idempotent.
    Uploading the same key twice overwrites the first object — it does NOT create a duplicate.
    This means if the generator crashes and restarts, re-uploading the
    same batch is completely safe. No duplicates in the lake.
    """
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=parquet_bytes,
        ContentType="application/octet-stream",
    )


def build_object_key(year, month, city, batch_num):
    """
    Builds the Hive-style partition key for MinIO storage.

    This is THE most important naming decision
    in the entire project. The folder structure IS the partition.

    year=2024/month=01/city=Mumbai/batch_0001.parquet

    Why this format?
    1. Hive convention — Spark, Athena, dbt all parse year=/month=/
       automatically for partition pruning
    2. Human readable — we can navigate the MinIO console and
       immediately understand the data layout
    3. Enables partition pruning — a query WHERE month='2024-01'
       AND city='Mumbai' reads ONLY matching folders
    4. city= allows city-level queries to skip all other cities

    The '=' in 'year=2024' is not decoration — it's the Hive
    partition format that query engines recognise.
    """
    city_clean = city.replace(" ", "_").replace("-", "_")
    return (
        f"year={year}/"
        f"month={month:02d}/"
        f"city={city_clean}/"
        f"batch_{batch_num:04d}.parquet"
    )


def main():
    log.info("=" * 60)
    log.info("UPI Payments Analytics — Data Generator")
    log.info("=" * 60)
    log.info(f"Target transactions : {NUM_TRANSACTIONS:,}")
    log.info(f"Date range          : {START_DATE} -> {END_DATE}")
    log.info(f"Batch size          : {BATCH_SIZE:,} rows per Parquet file")
    log.info(f"MinIO endpoint      : {MINIO_ENDPOINT}")
    log.info(f"Bucket              : {MINIO_BUCKET}")
    log.info("")

    # ----------------------------------------------------------
    # Initialise S3 client and bucket
    # ----------------------------------------------------------
    s3 = get_s3_client()
    ensure_bucket_exists(s3, MINIO_BUCKET)

    # ----------------------------------------------------------
    # Pre-compute lookup structures for fast generation
    # ----------------------------------------------------------
    city_names = [c[0] for c in CITIES]
    raw_city_weights = get_city_weights(CITIES)
    city_weights_norm = np.array(raw_city_weights)
    city_weights_norm = city_weights_norm / city_weights_norm.sum()

    merchant_codes = [m["category_code"] for m in MERCHANT_CATEGORIES]
    raw_merchant_weights = np.array([m["weight"] for m in MERCHANT_CATEGORIES], dtype=float)
    merchant_weights = raw_merchant_weights / raw_merchant_weights.sum()
    merchant_lookup = {m["category_code"]: m for m in MERCHANT_CATEGORIES}

    payment_codes = [p[0] for p in PAYMENT_TYPES]
    raw_payment_weights = np.array([p[2] for p in PAYMENT_TYPES], dtype=float)
    payment_weights = raw_payment_weights / raw_payment_weights.sum()

    app_names = [a[0] for a in UPI_APPS]
    raw_app_weights = np.array([a[3] for a in UPI_APPS], dtype=float)
    app_weights = raw_app_weights / raw_app_weights.sum()

    status_values = [s[0] for s in STATUSES]
    status_probs = np.array([s[1] for s in STATUSES])

    failure_reasons = [f[0] for f in FAILURE_REASONS]
    failure_probs = np.array([f[1] for f in FAILURE_REASONS])
    failure_probs = failure_probs / failure_probs.sum()

    device_types = [d[0] for d in DEVICE_TYPES]
    device_probs = np.array([d[1] for d in DEVICE_TYPES])

    hour_weights_arr = np.array(HOUR_WEIGHTS, dtype=float)
    hour_weights_norm = hour_weights_arr / hour_weights_arr.sum()

    # Build date range
    all_dates = build_date_range(START_DATE, END_DATE)
    total_days = len(all_dates)

    rng = np.random.default_rng(seed=42)

    # Distribute transactions across dates
    # Weekend and festival days get proportionally more
    date_weights = []
    for d in all_dates:
        w = 1.0
        if d.weekday() >= 5:
            w *= WEEKEND_MULTIPLIER
        if d.strftime("%Y-%m-%d") in FESTIVAL_DATES:
            w *= FESTIVAL_MULTIPLIER
        date_weights.append(w)

    date_weights_arr = np.array(date_weights)
    date_weights_norm = date_weights_arr / date_weights_arr.sum()

    # Assign transaction counts per date
    txns_per_date = np.round(
        date_weights_norm * NUM_TRANSACTIONS
    ).astype(int)

    # Adjust for rounding — make sure total equals NUM_TRANSACTIONS
    diff = NUM_TRANSACTIONS - txns_per_date.sum()
    txns_per_date[0] += diff

    # ----------------------------------------------------------
    # Main generation loop — date-by-date, batch-by-batch
    # ----------------------------------------------------------
    total_written = 0
    total_files = 0
    start_time = datetime.now()

    for date_idx, date in enumerate(all_dates):
        n_for_date = int(txns_per_date[date_idx])
        if n_for_date == 0:
            continue

        # Generate all transactions for this date at once
        day_df = generate_batch(
            rng=rng,
            batch_size=n_for_date,
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
            date=date,
            hour_weights_norm=hour_weights_norm,
        )

        # ----------------------------------------------------------
        # Split by city and write one Parquet file per city per day
        # groupby city BEFORE writing creates the city= partition naturally.
        # All Mumbai transactions for this date land in year=X/month=Y/city=Mumbai/
        # A query filtering city='Mumbai' skips all other city folders.
        # ----------------------------------------------------------
        for city, city_df in day_df.groupby("city"):
            # Split city's daily data into batches of BATCH_SIZE
            # ~4MB per file — avoids small files problem
            num_batches = max(1, len(city_df) // BATCH_SIZE)
            city_batches = np.array_split(city_df, num_batches)

            for batch_num, batch_df in enumerate(city_batches):
                if len(batch_df) == 0:
                    continue

                key = build_object_key(
                    year=date.year,
                    month=date.month,
                    city=city,
                    batch_num=batch_num,
                )

                parquet_bytes = df_to_parquet_bytes(batch_df)
                upload_to_minio(s3, MINIO_BUCKET, key, parquet_bytes)
                total_files += 1

        total_written += n_for_date

        # Progress logging every 30 days
        if (date_idx + 1) % 30 == 0 or date_idx == total_days - 1:
            elapsed = (datetime.now() - start_time).seconds
            pct = (date_idx + 1) / total_days * 100
            rate = total_written / max(elapsed, 1)
            log.info(
                f"Progress: {pct:5.1f}% | "
                f"Date: {date} | "
                f"Rows: {total_written:>10,} | "
                f"Files: {total_files:>6,} | "
                f"Rate: {rate:,.0f} rows/s"
            )

    # ----------------------------------------------------------
    # Summary
    # ----------------------------------------------------------
    elapsed_total = (datetime.now() - start_time).seconds
    log.info("")
    log.info("=" * 60)
    log.info("Generation complete")
    log.info(f"  Total transactions : {total_written:,}")
    log.info(f"  Total Parquet files: {total_files:,}")
    log.info(f"  Time elapsed       : {elapsed_total}s")
    log.info(f"  Avg throughput     : {total_written // max(elapsed_total, 1):,} rows/s")
    log.info("")
    log.info("Browse your data lake:")
    log.info("  MinIO console → http://localhost:9001")
    log.info(f"  Bucket → {MINIO_BUCKET}")
    log.info("  Structure: year=YYYY/month=MM/city=CCC/batch_NNNN.parquet")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
