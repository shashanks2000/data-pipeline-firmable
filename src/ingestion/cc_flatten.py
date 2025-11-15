#!/usr/bin/env python3
"""
parquet_commoncrawl_to_table.py

Reads parquet (or parquet.gz) files stored in `raw_data.file_data` (BYTEA),
parses CommonCrawl-style records and loads them into `commoncrawl_records`.

Requirements:
  pip install polars psycopg2-binary

Usage:
  python parquet_commoncrawl_to_table.py --pg "postgresql://user:pass@host:5432/db" --batch 5

Notes:
  - The script writes a temp parquet file per raw row, reads with Polars, converts types,
    writes a temporary CSV and uses psycopg2 COPY for high throughput.
  - It does not delete raw_data rows; if you want to mark processed rows, add UPDATE logic where indicated.
"""
import sys
import argparse
import os
import tempfile
import shutil
import json
from typing import List, Tuple
from urllib.parse import urlparse
import psycopg2
import polars as pl
from src import DB_CONFIG

DEFAULT_PG = DB_CONFIG
TARGET_TABLE = "commoncrawl_records"

def connect_pg(dsn: str):
    return psycopg2.connect(dsn)

def fetch_raw_parquet_rows(conn, limit: int):
    """
    Safely fetch up to `limit` rows from raw_data whose file_name looks like a parquet file.
    Returns a list of tuples: (id, file_name, file_data)
    """
    if limit is None:
        limit = 10
    cur = conn.cursor()
    try:
        sql = (
            "SELECT id, file_name, file_data "
            "FROM raw_data "
            "WHERE lower(file_name) LIKE %s OR lower(file_name) LIKE %s "
            "ORDER BY uploaded_at "
            "LIMIT %s"
        )
        params = ('%.parquet', '%.parquet.gz', limit)
        cur.execute(sql, params)
        rows = cur.fetchall()
        return rows
    except Exception as e:
        # Log more context for debugging
        print(f"[fetch_raw_parquet_rows] query failed: {e}", file=sys.stderr)
        raise
    finally:
        try:
            cur.close()
        except Exception:
            pass


def write_bytes_to_temp(path_bytes: bytes, suffix: str, tmp_dir: str) -> str:
    tf = tempfile.NamedTemporaryFile(delete=False, suffix=suffix, dir=tmp_dir)
    try:
        tf.write(path_bytes)
    finally:
        tf.close()
    return tf.name

def polars_read_parquet(path: str) -> pl.DataFrame:
    # Polars can read .parquet; if you saved a gzipped parquet, we decompress earlier.
    return pl.read_parquet(path)

def normalize_df_for_db(df: pl.DataFrame) -> pl.DataFrame:
    """
    Robust normalization:
      - convert polars DF -> pandas DF for easier per-column transformations
      - ensure expected columns exist
      - parse timestamp strings like '20080708222025' -> 'YYYY-MM-DD HH:MM:SS' (or None)
      - coerce numeric columns to nullable integers
      - return a Polars DataFrame with columns in the target order:
        urlkey, crawl_timestamp, status, url, filename, length, mime, offset, digest
    """
    import pandas as pd

    EXPECTED = ["urlkey", "timestamp", "status", "url", "filename", "length", "mime", "offset", "digest"]

    # 1) Convert to pandas for flexible transforms. If it's already small-ish this is fine.
    try:
        pdf = df.to_pandas()
    except Exception:
        # fallback: try converting via arrow
        pdf = df.to_arrow().to_pandas()

    # 2) Ensure expected columns exist
    for c in EXPECTED:
        if c not in pdf.columns:
            pdf[c] = None

    # 3) Parse timestamp strings of form YYYYMMDDhhmmss -> pandas.Timestamp (na if invalid)
    def parse_cc_timestamp(s):
        if s is None:
            return pd.NaT
        s = str(s).strip()
        if len(s) >= 14 and s.isdigit():
            try:
                return pd.to_datetime(f"{s[0:4]}-{s[4:6]}-{s[6:8]} {s[8:10]}:{s[10:12]}:{s[12:14]}")
            except Exception:
                return pd.NaT
        # try ISO parse fallback
        try:
            return pd.to_datetime(s, errors="coerce")
        except Exception:
            return pd.NaT

    pdf["crawl_timestamp"] = pdf["timestamp"].apply(parse_cc_timestamp)

    # 4) Coerce numeric columns to nullable integers (pandas Int64 dtype)
    for numcol in ("status", "length", "offset"):
        # convert empty strings to NaN then to nullable Int64
        pdf[numcol] = pd.to_numeric(pdf[numcol], errors="coerce").astype("Int64")

    # 5) Ensure string/text columns are strings (and preserve None/NaN as None)
    for txtcol in ("urlkey", "url", "filename", "mime", "digest"):
        pdf[txtcol] = pdf[txtcol].where(pd.notnull(pdf[txtcol]), None)
        # convert to str only for non-null entries
        pdf[txtcol] = pdf[txtcol].apply(lambda v: str(v) if v is not None else None)

    # 6) Select/rename final columns to expected target order
    final_cols = ["urlkey", "crawl_timestamp", "status", "url", "filename", "length", "mime", "offset", "digest"]
    result_pdf = pdf[final_cols].copy()

    # 7) Convert crawl_timestamp to string in ISO format (so COPY can parse) or keep as pandas datetime then convert via polars
    # We'll convert to ISO string 'YYYY-MM-DD HH:MM:SS' where not null, else None
    result_pdf["crawl_timestamp"] = result_pdf["crawl_timestamp"].apply(
        lambda ts: ts.strftime("%Y-%m-%d %H:%M:%S") if pd.notnull(ts) else None
    )

    # 8) Replace pd.NA/NA with None for safe conversion
    result_pdf = result_pdf.where(pd.notnull(result_pdf), None)

    # 9) Convert back to Polars DF and return
    try:
        final_pl = pl.from_pandas(result_pdf)
    except Exception:
        # as a fallback, convert column-by-column
        cols = {c: pl.Series(result_pdf[c].tolist()) for c in result_pdf.columns}
        final_pl = pl.DataFrame(cols)

    # Ensure final columns order is correct
    final_pl = final_pl.select(["urlkey", "crawl_timestamp", "status", "url", "filename", "length", "mime", "offset", "digest"])
    return final_pl

def write_dataframe_to_csv(df: pl.DataFrame, csv_path: str):
    # Polars writes header by default; Postgres COPY will use HEADER true
    df.write_csv(csv_path)

def copy_csv_to_db(conn, csv_path: str, raw_id: int, source_file: str, row_count: int):
    """
    Because we need to insert raw_id and source_file with each row, we will:
      - create a temp CSV where we preprend raw_id and source_file columns, or
      - use COPY into a staging table with the same column order. For simplicity, we will
        load into the target table with an explicit COPY column list expecting raw_id & source_file first.
    We'll generate a temp CSV that includes raw_id and source_file columns at front.
    """
    cur = conn.cursor()
    # build COPY sql matching columns order:
    # raw_id, source_file, urlkey, crawl_timestamp, status, url, filename, length, mime, offset, digest
    copy_sql = ("COPY {} (raw_id, source_file, urlkey, crawl_timestamp, status, url, filename, length, mime, file_offset, digest) "
                "FROM STDIN WITH (FORMAT csv, HEADER true)").format(TARGET_TABLE)
    with open(csv_path, "rb") as f:
        cur.copy_expert(copy_sql, f)
    conn.commit()
    cur.close()

def create_prefixed_csv(df: pl.DataFrame, raw_id: int, source_file: str, out_csv_path: str, tmp_dir: str):
    """
    Create a CSV with columns:
    raw_id,source_file,<df columns...>

    Polars ALWAYS writes headers, so we avoid 'has_header'.
    """
    import csv

    # 1) Write Polars DataFrame to a temporary CSV WITH header
    tmp_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv", dir=tmp_dir)
    tmp_csv.close()

    # Polars writes header by default (no has_header argument)
    df.write_csv(tmp_csv.name)

    # 2) Read that temp CSV and prepend raw_id, source_file
    final_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv", dir=tmp_dir)
    final_csv.close()

    with open(tmp_csv.name, "r", encoding="utf-8") as src, open(final_csv.name, "w", encoding="utf-8", newline="") as dst:
        reader = csv.reader(src)
        writer = csv.writer(dst)

        # header: add raw_id + source_file
        orig_header = next(reader)
        new_header = ["raw_id", "source_file"] + orig_header
        writer.writerow(new_header)

        # sanitize source_file (escape commas by quoting via csv module)
        for row in reader:
            writer.writerow([raw_id, source_file] + row)

    # cleanup the intermediate CSV
    os.unlink(tmp_csv.name)

    return final_csv.name


def process_raw_batch(conn, rows: List[Tuple[int,str,bytes]], tmp_dir: str):
    """
    Process a batch of raw_data rows (id, file_name, file_data bytes).
    Returns total inserted rows.
    """
    total_inserted = 0
    for row_id, file_name, file_bytes in rows:
        print(f"[proc] raw_id={row_id} file={file_name}")
        # save bytes to temp file (if gz, we name .parquet.gz and then decompress)
        is_gz = file_name.lower().endswith(".gz")
        suffix = ".parquet.gz" if is_gz else ".parquet"
        tmp_path = write_bytes_to_temp(file_bytes, suffix, tmp_dir)
        try:
            work_parquet = tmp_path
            if is_gz:
                # decompress to a new temp .parquet
                decompressed = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet", dir=tmp_dir)
                decompressed.close()
                import gzip
                with gzip.open(tmp_path, "rb") as inp, open(decompressed.name, "wb") as out:
                    shutil.copyfileobj(inp, out)
                work_parquet = decompressed.name

            # read with polars
            try:
                df = polars_read_parquet(work_parquet)
            except Exception as e:
                print(f"[error] polars read_parquet failed for {file_name}: {e}")
                # cleanup and continue
                if is_gz:
                    os.unlink(decompressed.name)
                continue

            # normalize types & columns
            try:
                final_df = normalize_df_for_db(df)
            except Exception as e:
                print(f"[error] normalize failed for {file_name}: {e}")
                if is_gz:
                    os.unlink(decompressed.name)
                continue

            # create prefixed CSV with raw metadata
            csv_path = create_prefixed_csv(final_df, row_id, file_name, os.path.join(tmp_dir, "csv_out"), tmp_dir)

            # copy to DB
            try:
                inserted_before = count_rows_in_table(conn, TARGET_TABLE)
                copy_csv_to_db(conn, csv_path, row_id, file_name, final_df.height)
                inserted_after = count_rows_in_table(conn, TARGET_TABLE)
                inserted = final_df.height  # assume all inserted
                print(f"[db] inserted {inserted} rows from {file_name}")
                total_inserted += inserted
            except Exception as e:
                print(f"[db error] COPY failed for {file_name}: {e}")
            finally:
                try: os.unlink(csv_path)
                except: pass

            if is_gz:
                os.unlink(decompressed.name)
        finally:
            try: os.unlink(tmp_path)
            except: pass
    return total_inserted

def count_rows_in_table(conn, table: str):
    cur = conn.cursor()
    cur.execute(f"SELECT count(*) FROM {table}")
    c = cur.fetchone()[0]
    cur.close()
    return c

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pg", default=DEFAULT_PG, help="psycopg2 DSN (postgresql://...)")
    parser.add_argument("--batch", type=int, default=5, help="raw_data rows per iteration")
    parser.add_argument("--keep", action="store_true", help="keep temp files")
    args = parser.parse_args()

    conn = connect_pg(args.pg)
    try:
        while True:
            rows = fetch_raw_parquet_rows(conn, args.batch)
            if not rows:
                print("No more parquet files to process. Exiting.")
                break
            tmp_dir = tempfile.mkdtemp(prefix="cc_parquet_")
            try:
                inserted = process_raw_batch(conn, rows, tmp_dir)
                print(f"[batch] inserted total {inserted} rows from {len(rows)} parquet files")
                # Optionally mark processed raw_data rows (e.g. add processed boolean in raw_data)
            finally:
                if args.keep:
                    print("Kept tmp dir for inspection:", tmp_dir)
                else:
                    shutil.rmtree(tmp_dir, ignore_errors=True)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
