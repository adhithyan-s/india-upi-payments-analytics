""" 
Reads partitioned Parquet files from MinIO (Bronze layer) and loads them into PostgreSQL raw.transactions table (Silver layer).
 
The loader is intentionally simple — no transformation happens here.
Cleaning and modelling is dbt's job. 
This script just moves data from the lake into the warehouse raw schema as faithfully as possible.
 
Usage:
    python loader/load_to_postgres.py
    make load
"""
 
import io
import os
import logging
import psycopg2
import psycopg2.extras
import boto3
import pandas as pd
from datetime import datetime
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# --- config from environment ---
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT",  "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET    = os.getenv("MINIO_BUCKET",    "upi-lake")
 
PG_HOST     = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT     = os.getenv("POSTGRES_PORT", "5432")
PG_DB       = os.getenv("POSTGRES_DB", "upidb")
PG_USER     = os.getenv("POSTGRES_USER", "upiuser")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "upipassword")

# how many rows to insert per batch — balances memory and speed
INSERT_BATCH_SIZE = 5000
 
 
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )
 
 
def get_pg_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def list_parquet_files(s3_client, bucket):
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(
        Bucket=bucket,
        PaginationConfig={"PageSize": 1000}
    )

    keys = []
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                keys.append(key)

    log.info(f"Paginated through all pages, found {len(keys)} files")
    return sorted(keys)


def read_parquet_from_minio(s3_client, bucket, key):
    """
    Downloads a single Parquet file from MinIO and returns a DataFrame.
 
    We download into memory (BytesIO) rather than writing to disk.
    For files of this size (~4MB each) that's faster and avoids leaving temp files around if the process crashes.
    """
    response = s3_client.get_object(Bucket=bucket, Key=key)
    parquet_bytes = response["Body"].read()
    return pd.read_parquet(io.BytesIO(parquet_bytes))


def clean_dataframe(df):
    """
    Light cleaning before loading to PostgreSQL.
 
    This is NOT the Silver transformation layer — dbt handles that.
    This is just making the data safe to insert:
    - cast types that PostgreSQL is strict about
    - handle NaN vs None (pandas uses NaN, psycopg2 needs None for NULL)
    - ensure dates are python date objects not numpy types
    """
    df = df.copy()
 
    # pandas uses NaN for missing strings, psycopg2 needs None
    df["failure_reason"] = df["failure_reason"].where(
        df["failure_reason"].notna(), None
    )
 
    # ensure txn_date is a python date (not numpy datetime64)
    df["txn_date"] = pd.to_datetime(df["txn_date"]).dt.date
 
    # ensure amount is a plain float
    df["amount_inr"] = df["amount_inr"].astype(float)
 
    # txn_hour as int
    df["txn_hour"] = df["txn_hour"].astype(int)
 
    # booleans
    df["is_weekend"] = df["is_weekend"].astype(bool)
    df["is_festival"] = df["is_festival"].astype(bool)
 
    return df


def get_already_loaded_keys(conn):
    """
    Returns the set of MinIO object keys that have already been recorded in the load tracking table.
 
    This is how we make the loader idempotent — we track which files we've already loaded and skip them on re-runs.
    Running the loader twice loads each file exactly once.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT object_key FROM raw.load_log
            WHERE status = 'success'
        """)
        rows = cur.fetchall()
    return {r[0] for r in rows}


def record_load(conn, key, row_count, status, error_msg=None):
    """Logs the result of loading one Parquet file."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO raw.load_log
                (object_key, row_count, status, error_message, loaded_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (object_key)
            DO UPDATE SET
                status        = EXCLUDED.status,
                row_count     = EXCLUDED.row_count,
                error_message = EXCLUDED.error_message,
                loaded_at     = NOW()
        """, (key, row_count, status, error_msg))
    conn.commit()


def insert_batch(cur, rows):
    """
    Inserts a batch of rows using execute_values — much faster than individual INSERT statements.
 
    ON CONFLICT DO NOTHING makes this idempotent:
    if a txn_id already exists in the table (from a previous run), we skip it rather than erroring or duplicating.
 
    This is idempotent loading — running this function twice on the same data produces the same table state.
    The primary key on txn_id is what makes it safe to re-run.
    """
    sql = """
        INSERT INTO raw.transactions (
            txn_id, amount_inr, city, state, merchant_category,
            upi_app, payment_type, status, failure_reason,
            txn_date, txn_hour, is_weekend, sender_upi,
            receiver_upi, device_type, created_at, loaded_at
        )
        VALUES %s
        ON CONFLICT (txn_id) DO NOTHING
    """
    psycopg2.extras.execute_values(cur, sql, rows, page_size=INSERT_BATCH_SIZE)


def df_to_rows(df, city_state_map):
    """Converts a cleaned DataFrame to a list of tuples for psycopg2."""
    rows = []
    loaded_at = datetime.utcnow()
 
    for _, r in df.iterrows():
        state = city_state_map.get(r["city"], "Unknown")
        rows.append((
            r["txn_id"],
            r["amount_inr"],
            r["city"],
            state,
            r["merchant_category"],
            r["upi_app"],
            r["payment_type"],
            r["status"],
            r["failure_reason"],
            r["txn_date"],
            r["txn_hour"],
            r["is_weekend"],
            r["sender_upi"],
            r["receiver_upi"],
            r["device_type"],
            r["created_at"],
            loaded_at,
        ))
    return rows


def ensure_load_log_table(conn):
    """
    Creates the load tracking table if it doesn't exist.
    This table records which Parquet files have been loaded so we can skip them on re-runs.
    """
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.load_log (
                object_key    VARCHAR(500) PRIMARY KEY,
                row_count     INT,
                status        VARCHAR(20),
                error_message TEXT,
                loaded_at     TIMESTAMPTZ DEFAULT NOW()
            )
        """)
 
        # also add primary key on raw.transactions if not already there
        # this is what makes ON CONFLICT (txn_id) DO NOTHING work
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'transactions_txn_id_pkey'
                      AND conrelid = 'raw.transactions'::regclass
                ) THEN
                    ALTER TABLE raw.transactions
                    ADD CONSTRAINT transactions_txn_id_pkey
                    PRIMARY KEY (txn_id);
                END IF;
            EXCEPTION WHEN others THEN
                NULL;  -- constraint already exists, ignore
            END $$;
        """)
    conn.commit()


def build_city_state_map():
    """Builds a city -> state lookup from config."""
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "generator"))
    from config import CITIES
    return {city: state for city, state, *_ in CITIES}


def main():
    log.info("=" * 60)
    log.info("UPI Loader — MinIO (Bronze) -> PostgreSQL (Silver)")
    log.info("=" * 60)
 
    s3 = get_s3_client()
    conn = get_pg_connection()
 
    ensure_load_log_table(conn)
    city_state_map = build_city_state_map()
 
    # find all parquet files in the lake
    log.info("Scanning MinIO bucket for Parquet files...")
    all_keys = list_parquet_files(s3, MINIO_BUCKET)
    log.info(f"Found {len(all_keys):,} Parquet files in {MINIO_BUCKET}")
 
    # skip files already loaded — idempotency
    already_loaded = get_already_loaded_keys(conn)
    keys_to_load = [k for k in all_keys if k not in already_loaded]
    log.info(f"Already loaded: {len(already_loaded):,} files")
    log.info(f"To load now:    {len(keys_to_load):,} files")
 
    if not keys_to_load:
        log.info("Nothing new to load. Exiting.")
        conn.close()
        return
 
    total_rows = 0
    failed_files = 0
    start = datetime.now()

    for i, key in enumerate(keys_to_load, 1):
        try:
            df = read_parquet_from_minio(s3, MINIO_BUCKET, key)
            df = clean_dataframe(df)
            rows = df_to_rows(df, city_state_map)
 
            with conn.cursor() as cur:
                insert_batch(cur, rows)
            conn.commit()
 
            record_load(conn, key, len(rows), "success")
            total_rows += len(rows)
 
            if i % 50 == 0 or i == len(keys_to_load):
                elapsed = (datetime.now() - start).seconds
                rate = total_rows / max(elapsed, 1)
                pct = i / len(keys_to_load) * 100
                log.info(
                    f"{pct:5.1f}% | file {i:>5}/{len(keys_to_load)} | "
                    f"rows loaded: {total_rows:>10,} | {rate:,.0f} rows/s"
                )
 
        except Exception as e:
            conn.rollback()
            log.error(f"Failed to load {key}: {e}")
            record_load(conn, key, 0, "failed", str(e))
            failed_files += 1
            continue

    elapsed_total = (datetime.now() - start).seconds
    log.info("")
    log.info("=" * 60)
    log.info("Load complete")
    log.info(f"  Rows loaded  : {total_rows:,}")
    log.info(f"  Files loaded : {len(keys_to_load) - failed_files:,}")
    log.info(f"  Files failed : {failed_files:,}")
    log.info(f"  Time elapsed : {elapsed_total}s")
    log.info("=" * 60)
 
    conn.close()
 
 
if __name__ == "__main__":
    main()