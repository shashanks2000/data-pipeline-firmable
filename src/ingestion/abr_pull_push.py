#!/usr/bin/env python3
"""
abr_pull_push.py

- Downloads ABR ZIP URLs (parallel)
- Extracts ZIPs
- Finds XML / .xml.gz files
- Processes each XML file in a separate process (ProcessPoolExecutor)
- Streams <ABR> records with ElementTree.iterparse (low memory)
- Uses Polars to create CSV batches and psycopg2 COPY for fast bulk inserts

Usage:
  pip install polars requests psycopg2-binary
  python abr_pull_push.py --workers 4 --batch 8000

Defaults include the two public_split URLs and your Postgres DSN (airflow).
"""

import argparse
import concurrent.futures
import gzip
import os
import shutil
import sys
import tempfile
import zipfile
from datetime import datetime
import xml.etree.ElementTree as ET
from urllib.parse import urlparse

import requests
import polars as pl
import psycopg2
from src import DB_CONFIG

# ---------------------- Defaults / Config ----------------------
DEFAULT_URLS = [
    "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/resource/0ae4d427-6fa8-4d40-8e76-c6909b5a071b/download/public_split_1_10.zip",
    "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/resource/635fcb95-7864-4509-9fa7-a62a6e32b62d/download/public_split_11_20.zip",
]
DEFAULT_PG = DB_CONFIG
DEFAULT_WORKERS = max(1, (os.cpu_count() or 2) - 1)
DEFAULT_BATCH = 8000

COPY_COLUMNS = [
    "file_sequence_number","record_count","extract_time",
    "record_last_updated_date","replaced",
    "abn","abn_status","abn_status_from_date",
    "entity_type_ind","entity_type_text",
    "mainentity_name_type","mainentity_name_text",
    "businessaddress_state","businessaddress_postcode",
    "asicnumber","asicnumber_type",
    "gst_status","gst_status_from_date",
    "other_entities"
]

# ---------------------- Helpers ----------------------
def sqlalchemy_to_psycopg2_conninfo(dsn: str) -> str:
    """
    Convert sqlalchemy-ish DSN to psycopg2 conninfo string:
    e.g. postgresql+psycopg2://user:pass@host:port/db -> "host=... port=... user=... password=... dbname=..."
    """
    if dsn.startswith("postgresql+psycopg2://"):
        dsn = "postgresql://" + dsn.split("://", 1)[1]
    parsed = urlparse(dsn)
    user = parsed.username or ""
    password = parsed.password or ""
    host = parsed.hostname or "localhost"
    port = parsed.port or 5432
    db = (parsed.path[1:] if parsed.path and parsed.path.startswith("/") else parsed.path)
    return f"host={host} port={port} user={user} password={password} dbname={db}"

def safe_text(elem):
    if elem is None or elem.text is None:
        return None
    t = elem.text.strip()
    return t if t != "" else None

def parse_yyyymmdd_to_iso(s):
    if not s:
        return None
    s = s.strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    try:
        return datetime.fromisoformat(s).date().isoformat()
    except Exception:
        return None

def download_file(url, dest_path, timeout=180):
    print(f"[download] {url} -> {dest_path}")
    with requests.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    print(f"[download] done: {dest_path}")

def extract_zip(zip_path, extract_to):
    print(f"[extract] {zip_path} -> {extract_to}")
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(extract_to)
    print(f"[extract] done: {extract_to}")

def find_xmls(root_dir):
    out = []
    for root, _, files in os.walk(root_dir):
        for fn in files:
            low = fn.lower()
            if low.endswith(".xml") or low.endswith(".xml.gz") or low.endswith(".gz"):
                out.append(os.path.join(root, fn))
    return out

# ---------------------- XML streaming ----------------------
def stream_rows_from_path(path):
    """
    Generator yielding dictionaries matching COPY_COLUMNS.
    Uses iterparse and clears elements to keep memory low.
    """
    transfer_info = {"FileSequenceNumber": None, "RecordCount": None, "ExtractTime": None}
    context = ET.iterparse(path, events=("start","end"))
    _, root = next(context)
    for event, elem in context:
        tag = elem.tag
        if tag == "TransferInfo" and event == "end":
            for ch in list(elem):
                if ch.tag in transfer_info:
                    transfer_info[ch.tag] = safe_text(ch)
            elem.clear()
        if tag == "ABR" and event == "end":
            abr = elem
            row = {
                "file_sequence_number": int(transfer_info.get("FileSequenceNumber")) if transfer_info.get("FileSequenceNumber") else None,
                "record_count": int(transfer_info.get("RecordCount")) if transfer_info.get("RecordCount") else None,
            }
            ext = transfer_info.get("ExtractTime")
            try:
                row["extract_time"] = datetime.fromisoformat(ext).isoformat() if ext else None
            except Exception:
                row["extract_time"] = ext

            row["record_last_updated_date"] = parse_yyyymmdd_to_iso(abr.attrib.get("recordLastUpdatedDate"))
            row["replaced"] = abr.attrib.get("replaced")

            abn = abr.find("ABN")
            row["abn"] = safe_text(abn)
            row["abn_status"] = abn.attrib.get("status") if abn is not None else None
            row["abn_status_from_date"] = parse_yyyymmdd_to_iso(abn.attrib.get("ABNStatusFromDate")) if abn is not None else None

            ent = abr.find("EntityType")
            row["entity_type_ind"] = safe_text(ent.find("EntityTypeInd")) if ent is not None else None
            row["entity_type_text"] = safe_text(ent.find("EntityTypeText")) if ent is not None else None

            main = abr.find("MainEntity")
            if main is not None:
                non = main.find(".//NonIndividualName")
                row["mainentity_name_type"] = non.attrib.get("type") if non is not None else None
                row["mainentity_name_text"] = safe_text(non.find("NonIndividualNameText")) if non is not None else None
                addr = main.find(".//BusinessAddress/AddressDetails")
                if addr is not None:
                    row["businessaddress_state"] = safe_text(addr.find("State"))
                    row["businessaddress_postcode"] = safe_text(addr.find("Postcode"))
                else:
                    row["businessaddress_state"] = None
                    row["businessaddress_postcode"] = None
            else:
                row["mainentity_name_type"] = None
                row["mainentity_name_text"] = None
                row["businessaddress_state"] = None
                row["businessaddress_postcode"] = None

            asic = abr.find("ASICNumber")
            row["asicnumber"] = safe_text(asic)
            row["asicnumber_type"] = asic.attrib.get("ASICNumberType") if asic is not None else None

            gst = abr.find("GST")
            row["gst_status"] = gst.attrib.get("status") if gst is not None else None
            row["gst_status_from_date"] = parse_yyyymmdd_to_iso(gst.attrib.get("GSTStatusFromDate")) if gst is not None else None

            others = []
            for oe in abr.findall("OtherEntity"):
                non = oe.find(".//NonIndividualName")
                if non is not None:
                    typ = non.attrib.get("type")
                    text = safe_text(non.find("NonIndividualNameText"))
                    if text:
                        others.append(f"{typ}:{text}" if typ else text)
            row["other_entities"] = "; ".join(others) if others else None

            elem.clear()
            yield row

# ---------------------- Polars -> COPY ----------------------
def polars_batch_copy(conn, rows, tmp_dir):
    """
    Convert rows -> Polars DataFrame -> CSV (temp file) -> COPY into Postgres.
    Returns number of rows inserted.
    This version:
      - builds column lists explicitly to avoid mixed-type inference errors
      - writes CSV with header and uses COPY ... WITH (FORMAT csv, HEADER true)
    """
    if not rows:
        return 0

    # Number of rows
    n = len(rows)

    # Build column lists in the exact order of COPY_COLUMNS
    col_data = {}
    for c in COPY_COLUMNS:
        # collect value or None for each row
        col_data[c] = [r.get(c) if (c in r) else None for r in rows]

    # Create Polars Series with explicit dtype for numeric columns
    # Use nullable Int64 for integers, Utf8 for everything else.
    series_dict = {}
    for c, data in col_data.items():
        if c in ("file_sequence_number", "record_count"):
            # convert empty strings to None, keep ints if present, else None
            conv = []
            for v in data:
                if v is None or (isinstance(v, str) and v.strip() == ""):
                    conv.append(None)
                else:
                    # try convert to int if it's a numeric string
                    try:
                        conv.append(int(v))
                    except Exception:
                        # fallback: if it's not numeric, set None (we prefer nulls over wrong type)
                        conv.append(None)
            series_dict[c] = pl.Series(c, conv, dtype=pl.Int64)
        else:
            # Ensure everything is string or None
            conv = []
            for v in data:
                if v is None:
                    conv.append(None)
                else:
                    # convert non-str to str, preserve leading zeros
                    conv.append(str(v))
            series_dict[c] = pl.Series(c, conv, dtype=pl.Utf8)

    # Build DataFrame
    df = pl.DataFrame(series_dict)

    # Ensure column order (should already match)
    df = df.select(COPY_COLUMNS)

    # Write CSV (with header) to a temporary file
    with tempfile.NamedTemporaryFile(mode="w+b", delete=False, dir=tmp_dir, suffix=".csv") as tf:
        csv_path = tf.name
    try:
        # Polars will write header by default
        df.write_csv(csv_path)

        # COPY with HEADER true to match written CSV
        with open(csv_path, "rb") as f:
            cur = conn.cursor()
            copy_sql = ("COPY abr_flattened (file_sequence_number, record_count, extract_time, record_last_updated_date, replaced, "
                        "abn, abn_status, abn_status_from_date, entity_type_ind, entity_type_text, mainentity_name_type, "
                        "mainentity_name_text, businessaddress_state, businessaddress_postcode, asicnumber, asicnumber_type, "
                        "gst_status, gst_status_from_date, other_entities) FROM STDIN WITH (FORMAT csv, HEADER true)")
            cur.copy_expert(copy_sql, f)
            conn.commit()
            cur.close()
        return df.height
    finally:
        try:
            os.unlink(csv_path)
        except Exception:
            pass


# ---------------------- Worker process ----------------------
def worker_process_file(path, pg_conninfo, batch_size):
    """
    Runs inside a separate process. Decompresses if needed, streams rows, batches to Polars and COPYs.
    Returns number of rows inserted.
    """
    tmp_dir = tempfile.mkdtemp(prefix="abr_worker_")
    conn = psycopg2.connect(pg_conninfo)
    inserted = 0
    try:
        if path.lower().endswith(".gz"):
            temp_xml = os.path.join(tmp_dir, "decompressed.xml")
            with gzip.open(path, "rb") as gz_in, open(temp_xml, "wb") as out:
                shutil.copyfileobj(gz_in, out)
            source_path = temp_xml
        else:
            source_path = path

        buffer = []
        for row in stream_rows_from_path(source_path):
            buffer.append(row)
            if len(buffer) >= batch_size:
                n = polars_batch_copy(conn, buffer, tmp_dir)
                inserted += n
                buffer = []
        if buffer:
            n = polars_batch_copy(conn, buffer, tmp_dir)
            inserted += n
    finally:
        try:
            conn.close()
        except Exception:
            pass
        shutil.rmtree(tmp_dir, ignore_errors=True)
    return inserted

# ---------------------- Main ----------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--urls", default=",".join(DEFAULT_URLS), help="Comma-separated ZIP URLs")
    parser.add_argument("--pg", default=DEFAULT_PG, help="SQLAlchemy-ish DSN (postgresql+psycopg2://... or postgresql://...)")
    parser.add_argument("--work-dir", default=None, help="Work directory to keep downloads/extracted files (optional)")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Number of parallel processes")
    parser.add_argument("--batch", type=int, default=DEFAULT_BATCH, help="Rows per COPY batch")
    args = parser.parse_args()

    urls = [u.strip() for u in args.urls.split(",") if u.strip()]
    if not urls:
        print("No URLs provided", file=sys.stderr)
        sys.exit(2)

    # Prepare work directory
    if args.work_dir:
        work_dir = os.path.abspath(args.work_dir)
        os.makedirs(work_dir, exist_ok=True)
        remove_tmp = False
    else:
        work_dir = tempfile.mkdtemp(prefix="abr_work_")
        remove_tmp = True

    downloads = os.path.join(work_dir, "downloads"); os.makedirs(downloads, exist_ok=True)
    extracts = os.path.join(work_dir, "extracted"); os.makedirs(extracts, exist_ok=True)

    # Convert DSN to psycopg2 conninfo
    pg_conninfo = sqlalchemy_to_psycopg2_conninfo(args.pg)

    # 1) Download ZIPs in parallel (thread pool)
    zip_paths = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(urls), 4)) as dl_pool:
        future_map = {}
        for i, u in enumerate(urls):
            dest = os.path.join(downloads, f"download_{i+1}.zip")
            future = dl_pool.submit(download_file, u, dest)
            future_map[future] = dest
        for fut in concurrent.futures.as_completed(future_map):
            dest = future_map[fut]
            try:
                fut.result()
                zip_paths.append(dest)
            except Exception as e:
                print(f"[download error] {e}", file=sys.stderr)

    if not zip_paths:
        print("No zip files downloaded; exiting", file=sys.stderr)
        if remove_tmp:
            shutil.rmtree(work_dir, ignore_errors=True)
        sys.exit(2)

    # 2) Extract zips
    for zp in zip_paths:
        sub = os.path.join(extracts, os.path.splitext(os.path.basename(zp))[0])
        os.makedirs(sub, exist_ok=True)
        extract_zip(zp, sub)

    # 3) Find XML files
    xml_files = find_xmls(extracts)
    if not xml_files:
        print("No XML files found", file=sys.stderr)
        if remove_tmp:
            shutil.rmtree(work_dir, ignore_errors=True)
        sys.exit(2)

    print(f"[main] Found {len(xml_files)} xml files. Launching {args.workers} worker processes (batch={args.batch}).")

    # 4) Process files in parallel using ProcessPoolExecutor
    totals = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as pool:
        fut_map = {pool.submit(worker_process_file, xf, pg_conninfo, args.batch): xf for xf in xml_files}
        for fut in concurrent.futures.as_completed(fut_map):
            xf = fut_map[fut]
            try:
                inserted = fut.result()
                totals.append(inserted)
                print(f"[main] finished {os.path.basename(xf)} -> inserted {inserted} rows")
            except Exception as e:
                print(f"[error] worker failed for {xf}: {e}", file=sys.stderr)

    grand_total = sum(totals)
    print(f"[main] ALL DONE. Total rows inserted: {grand_total}")

    if remove_tmp:
        shutil.rmtree(work_dir, ignore_errors=True)
    else:
        print(f"[main] work dir retained: {work_dir}")

if __name__ == "__main__":
    main()
