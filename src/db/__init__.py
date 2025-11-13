from sqlalchemy import create_engine
from sqlalchemy.orm import Session, declarative_base

db_config = "postgresql+psycopg2://airflow:airflow@localhost:5432/appdb"
engine = create_engine(
    db_config,
    pool_size=20,
)

def get_db():
    with Session(engine) as session:
        yield session
