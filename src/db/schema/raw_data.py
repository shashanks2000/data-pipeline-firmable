import json
from datetime import datetime
from sqlalchemy import Column, BigInteger, JSON, TIMESTAMP, func
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel, ConfigDict

Base = declarative_base()

class RawData(Base):
    __tablename__ = "raw_data"

    id = Column(BigInteger, primary_key=True)
    data = Column(JSON, nullable=False)
    created_at = Column(TIMESTAMP, server_default=func.now())


class RawData(BaseModel):

    id: int
    data: json
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)