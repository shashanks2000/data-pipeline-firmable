# models.py
from typing import Optional
from sqlalchemy import Column, Text, TIMESTAMP, func, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from typing import Optional
from datetime import datetime
from pydantic import BaseModel, ConfigDict

Base = declarative_base()

class IngestionHistory(Base):
    __tablename__ = "ingestion_history"

    cc_index = Column(Text, primary_key=True, nullable=False)  # e.g. 'CC-MAIN-2025-43'
    bookmark = Column(BigInteger, nullable=True, default=0)
    created_at = Column(TIMESTAMP, server_default=func.now(), nullable=False)
    last_updated = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now(), nullable=False)


class IngestionHistorySchema(BaseModel):
    cc_index: str 
    bookmark: int
    created_at: Optional[datetime]
    last_updated: Optional[datetime]

    model_config = ConfigDict(from_attributes=True)

