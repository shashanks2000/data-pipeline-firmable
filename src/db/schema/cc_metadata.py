from sqlalchemy import Column, Text, TIMESTAMP, func
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel, ConfigDict, Field
from datetime import datetime
from typing import Optional

Base = declarative_base()

class CommonCrawlMetadata(Base):
    __tablename__ = "commoncrawl_metadata"

    id = Column(Text, primary_key=True)
    name = Column(Text)
    timegate = Column(Text)
    cdx_api = Column(Text)
    crawl_start = Column(TIMESTAMP)
    crawl_end = Column(TIMESTAMP)
    created_at = Column(TIMESTAMP, server_default=func.now())


class CommonCrawlMetadataSchema(BaseModel):
    id: str
    name: Optional[str] = None
    timegate: Optional[str] = None
    cdx_api: Optional[str] = Field(None, alias="cdx-api")
    crawl_start: Optional[datetime] = Field(None, alias="from")
    crawl_end: Optional[datetime] = Field(None, alias="to")
    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)

