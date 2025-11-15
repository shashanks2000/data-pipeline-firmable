import requests
import json
import polars as pl
import pyarrow.parquet as pq
import gzip
from io import BytesIO
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy import func, select

from src.db.schema.raw_data import RawData
from src.db.schema.cc_metadata import CommonCrawlMetadata
from src.db.schema.ingestion_history import IngestionHistory, IngestionHistorySchema
from src.db import get_db


def segment_cc_index(url: str) -> str:
    """
    Extract Common Crawl segment name from URL.
    """
    return url.rstrip("/").split("/")[-1].replace("-index", "")


def get_all_urls(db: Session) -> list[str]:
    """
    Fetch all Common Crawl API endpoints.
    """
    rows = db.execute(select(CommonCrawlMetadata.cdx_api)).all()
    return [r[0] for r in rows]


def get_latest_bookmark(cc_index: str, db: Session) -> int:
    """
    Fetch latest bookmark for a given Common Crawl index.
    """
    return db.scalar(
        select(func.coalesce(IngestionHistory.bookmark, 0))
        .where(IngestionHistory.cc_index == cc_index)
    ) or 0


def log_bookmark(db: Session, cc_index: str, bookmark: int):
    """
    Upsert bookmark in ingestion history.
    """
    # Ensure bookmark never goes negative
    safe_bookmark = max(0, bookmark)
    
    log = IngestionHistorySchema(
        cc_index=cc_index,
        bookmark=safe_bookmark,
        created_at=datetime.now(timezone.utc),
        last_updated=datetime.now(timezone.utc),
    )
    db.merge(IngestionHistory(**log.model_dump(exclude={"created_at", "last_updated"})))
    db.commit()
    print(f"📘 Logged bookmark={safe_bookmark} for {cc_index}")


def save_records_as_gzipped_parquet(records, segment: str, page: int, db: Session):
    """
    Convert records -> Parquet -> Gzip -> Insert RawData.
    """
    if not records:
        return

    df = pl.DataFrame(records)
    table = df.to_arrow()

    # Convert to parquet bytes
    buffer = BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)
    parquet_bytes = buffer.getvalue()

    # Compress
    gzip_buffer = BytesIO()
    filename = f"{segment}-{page}.parquet.gz"
    with gzip.GzipFile(fileobj=gzip_buffer, mode="wb") as gz_file:
        gz_file.write(parquet_bytes)
    gzip_buffer.seek(0)
    gzipped_data = gzip_buffer.getvalue()

    # Insert into DB
    insert = RawData(
        file_name=filename,
        file_data=gzipped_data,
        file_size=len(gzipped_data),
    )
    db.add(insert)
    db.commit()
    print(f"✅ Saved {filename} ({len(gzipped_data)/1e6:.2f} MB)")


def download_data_from_each_index(db: Session = next(get_db())):
    """
    Main pipeline: loop through all Common Crawl indices and download pages incrementally.
    """
    urls = get_all_urls(db)
    # urls = ['https://index.commoncrawl.org/CC-MAIN-2025-43-index']
    # urls.sort()

    for url in urls:
        segment = segment_cc_index(url)
        page = get_latest_bookmark(segment, db)
        # page = 2230
        print(f"\n🚀 Starting segment={segment} from page={page}")

        while True:
            uri = f"{url}?url=*.au&output=json&filter=statuscode:200&page={page}"
            print(f"Fetching {uri}")

            try:
                response = requests.get(uri, stream=True, timeout=60)
                if response.status_code == 400:
                    try:
                        msg = response.json().get("message")
                        if msg:
                            # Only log if we've made progress (page > 0)
                            if page > 0:
                                log_bookmark(db, segment, page - 1)
                            break
                    except json.JSONDecodeError:
                        if page > 0:
                            log_bookmark(db, segment, page - 1)
                        break

                response.raise_for_status()

                # Stream lines
                records = [json.loads(line) for line in response.iter_lines(decode_unicode=True) if line]

                if not records:
                    # Only log if we've made progress (page > 0)
                    if page > 0:
                        log_bookmark(db, segment, page - 1)
                    break

                save_records_as_gzipped_parquet(records, segment, page, db)
                page += 1

            except requests.exceptions.ChunkedEncodingError:
                print(f"Connection dropped on page {page}, retrying...")
                continue  # retry same page

            except requests.exceptions.RequestException as e:
                print(f"Request error: {e}")
                if page > 0:
                    log_bookmark(db, segment, page - 1)
                break

            except Exception as e:
                db.rollback()
                print(f"Fatal error on {segment}, page {page}: {e}")
                if page > 0:
                    log_bookmark(db, segment, page - 1)
                break

        print(f"Completed segment={segment} last bookmark={page - 1 if page > 0 else 0}")
