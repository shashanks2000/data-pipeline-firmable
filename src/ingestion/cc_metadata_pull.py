import requests
from typing import List
from src import COMMONCRAWL_METADATA_URL
from src.db import get_db
from src.db.schema.cc_metadata import CommonCrawlMetadata, CommonCrawlMetadataSchema


def download_cc_metadata() -> List[CommonCrawlMetadataSchema]:
    response = requests.get(COMMONCRAWL_METADATA_URL, timeout=15)  
    response.raise_for_status()
    data = response.json()
    return [CommonCrawlMetadataSchema.model_validate(each) for each in data]


def push_metadata_to_postgres(db = next(get_db())):
    try:
        data = download_cc_metadata()
        for item in data:
            db.merge(CommonCrawlMetadata(**item.model_dump(by_alias=False, exclude_none=True, exclude={"created_at"})))
        db.commit()

    except Exception as e:
        print(str(e))
