import json
from datetime import datetime
from sqlalchemy import Column, BigInteger, Text, TIMESTAMP, func, LargeBinary
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel, ConfigDict

Base = declarative_base()

class RawData(Base):
    __tablename__ = "raw_data"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    file_name = Column(Text, nullable=False)
    file_data = Column(LargeBinary, nullable=False)
    file_size = Column(BigInteger, primary_key=True)
    uploaded_at = Column(TIMESTAMP, server_default=func.now())

class RawDataSchema(BaseModel):

    id: int
    filen_ame: str
    file_data: bytes
    file_size: int
    uploaded_at: datetime

    model_config = ConfigDict(from_attributes=True)