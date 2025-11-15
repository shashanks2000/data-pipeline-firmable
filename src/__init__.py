import os
from dotenv import load_dotenv

load_dotenv()

COMMONCRAWL_METADATA_URL = os.getenv("COMMONCRAWL_METADATA_URL")
DB_CONFIG = os.getenv('DB_CONFIG')