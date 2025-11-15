from sqlalchemy import create_engine
from sqlalchemy.orm import Session, declarative_base
from src import DB_CONFIG

db_config = DB_CONFIG
engine = create_engine(
    db_config,
    pool_size=20,
)

def get_db():
    with Session(engine) as session:
        yield session
