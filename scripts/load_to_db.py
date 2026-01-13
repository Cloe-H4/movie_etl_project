import os
import logging
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

# =========================
# PATH CONFIGURATION (AIRFLOW SAFE)
# =========================
AIRFLOW_HOME = "/opt/airflow"
DATA_DIR = os.path.join(AIRFLOW_HOME, "data", "raw")
LOG_DIR = os.path.join(AIRFLOW_HOME, "logs")

MOVIES_CSV_PATH = os.path.join(DATA_DIR, "movies.csv")
RATINGS_CSV_PATH = os.path.join(DATA_DIR, "ratings.csv")

# =========================
# SETUP LOGGING
# =========================
os.makedirs(LOG_DIR, exist_ok=True)

log_file_path = os.path.join(LOG_DIR, "load_to_db.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=log_file_path,
    filemode="w"
)

# =========================
# ENVIRONMENT VARIABLES
# =========================
load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# =========================
# FILE EXISTENCE CHECK
# =========================
logging.info(f"Movies CSV exists: {os.path.exists(MOVIES_CSV_PATH)}")
logging.info(f"Ratings CSV exists: {os.path.exists(RATINGS_CSV_PATH)}")

# =========================
# LOAD MOVIES DATA
# =========================
chunksize = 50000
first_chunk = True

logging.info("Loading movies data to database...")

for chunk in pd.read_csv(MOVIES_CSV_PATH, chunksize=chunksize):
    try:
        chunk.to_sql(
            "stg_movies",
            engine,
            if_exists="replace" if first_chunk else "append",
            index=False,
            method="multi"
        )
        first_chunk = False
        logging.info(f"Loaded {len(chunk)} movie rows")
    except Exception as e:
        logging.error(f"Error loading movies chunk: {e}")
        raise

logging.info("Movies data loaded successfully.")

# =========================
# LOAD RATINGS DATA
# =========================
first_chunk = True

logging.info("Loading ratings data to database...")

for chunk in pd.read_csv(RATINGS_CSV_PATH, chunksize=chunksize):
    try:
        chunk.to_sql(
            "stg_ratings",
            engine,
            if_exists="replace" if first_chunk else "append",
            index=False,
            method="multi"
        )
        first_chunk = False
        logging.info(f"Loaded {len(chunk)} ratings rows")
    except Exception as e:
        logging.error(f"Error loading ratings chunk: {e}")
        raise

logging.info("Ratings data loaded successfully.")
logging.info("Data loading to database completed successfully.")
