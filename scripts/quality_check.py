import pandas as pd
import logging
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# AIRFLOW PATH CONFIG
AIRFLOW_HOME = "/opt/airflow"
LOG_DIR = os.path.join(AIRFLOW_HOME, "logs")

os.makedirs(LOG_DIR, exist_ok=True)

# LOGGING SETUP
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "quality_check.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# CONNECT TO DATABASE
load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

logging.info("Connected to database for quality checks")

# ROW COUNT VALIDATION (MOVIES)
logging.info("Starting Movies row count validation...")

movies_raw_count = pd.read_sql(
    "SELECT COUNT(*) AS count FROM stg_movies",
    engine
)["count"][0]

movies_clean_count = pd.read_sql(
    "SELECT COUNT(*) AS count FROM stg_movies_clean",
    engine
)["count"][0]

logging.info(f"Movies raw data count: {movies_raw_count}")
logging.info(f"Movies clean data count: {movies_clean_count}")

if movies_clean_count > movies_raw_count:
    logging.error("Error: Movies clean data count is greater than raw data count")
    raise Exception("Movies row count validation failed")

# ROW COUNT VALIDATION (RATINGS)
logging.info("Starting Ratings row count validation...")

ratings_raw_count = pd.read_sql(
    "SELECT COUNT(*) AS count FROM stg_ratings",
    engine
)["count"][0]

ratings_clean_count = pd.read_sql(
    "SELECT COUNT(*) AS count FROM stg_ratings_clean",
    engine
)["count"][0]

logging.info(f"Ratings raw data count: {ratings_raw_count}")
logging.info(f"Ratings clean data count: {ratings_clean_count}")

if ratings_clean_count > ratings_raw_count:
    logging.error("Error: Ratings clean data count is greater than raw data count")
    raise Exception("Ratings row count validation failed")

logging.info("Row count validation passed :)")

# ORPHAN RATINGS VALIDATION
logging.info("Starting Orphan count validation...")

orphan_query = """
    SELECT COUNT(*) AS count
    FROM stg_ratings_clean r
    LEFT JOIN stg_movies_clean m
        ON r.movie_id = m.movie_id
    WHERE m.movie_id IS NULL
"""

orphan_count = pd.read_sql(orphan_query, engine)["count"][0]

logging.info(f"Orphan count is {orphan_count}")

if orphan_count != 0:
    logging.error("Ratings without corresponding movies exist")
    raise Exception("Orphan count validation failed")

logging.info("Orphan count validation passed :)")

# RATING RANGE VALIDATION
logging.info("Starting Rating Range validation...")

rating_range_query = """
    SELECT 
        MIN(rating) AS min_rating,
        MAX(rating) AS max_rating
    FROM stg_ratings_clean
"""

rating_stats = pd.read_sql(rating_range_query, engine)

min_rating = rating_stats["min_rating"][0]
max_rating = rating_stats["max_rating"][0]

logging.info(f"Min rating: {min_rating}, Max rating: {max_rating}")

if min_rating < 0.5 or max_rating > 5.0:
    logging.error("Ratings out of valid range detected")
    raise Exception("Rating range validation failed")

logging.info("Rating range validation passed :)")
logging.info("All quality checks passed successfully!")
