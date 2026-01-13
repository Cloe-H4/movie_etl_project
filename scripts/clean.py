import os
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# AIRFLOW PATH CONFIG
AIRFLOW_HOME = "/opt/airflow"
LOG_DIR = os.path.join(AIRFLOW_HOME, "logs")

os.makedirs(LOG_DIR, exist_ok=True)

# LOGGING SETUP
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "clean.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# DATABASE CONNECTION
load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# SQL STATEMENTS
drop_movies_clean = text("DROP TABLE IF EXISTS stg_movies_clean;")

create_movies_clean = text("""
CREATE TABLE stg_movies_clean AS
SELECT DISTINCT 
        "movieId" AS movie_id,
        title,
        genres
FROM stg_movies
WHERE "movieId" IS NOT NULL
      AND title IS NOT NULL;
""")

drop_ratings_clean = text("DROP TABLE IF EXISTS stg_ratings_clean;")

create_ratings_clean = text("""
CREATE TABLE stg_ratings_clean AS
SELECT DISTINCT 
        "userId" AS user_id,
        "movieId" AS movie_id,
        rating,
        timestamp
FROM stg_ratings
WHERE "userId" IS NOT NULL
      AND "movieId" IS NOT NULL
      AND rating BETWEEN 0.5 AND 5.0;
""")

# EXECUTION
logging.info("Starting data cleaning process")

try:
    with engine.begin() as conn:

        logging.info("Dropping stg_movies_clean if it exists")
        conn.execute(drop_movies_clean)

        logging.info("Creating stg_movies_clean table")
        conn.execute(create_movies_clean)

        logging.info("Dropping stg_ratings_clean if it exists")
        conn.execute(drop_ratings_clean)

        logging.info("Creating stg_ratings_clean table")
        conn.execute(create_ratings_clean)

except Exception as e:
    logging.error(f"Error during data cleaning: {e}")
    raise

logging.info("Data cleaning process completed successfully")
