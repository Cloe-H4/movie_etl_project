import pandas as pd
import logging
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# setup logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename=os.path.join("logs", "quality_check.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# connect to database
load_dotenv()
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
logging.info("Connected to database for quality checks")
# checking movies row count
logging.info("Starting Row Count validation...")

raw_count = pd.read_sql("SELECT COUNT(*) as count FROM stg_movies", engine)["count"][0]
clean_count = pd.read_sql("SELECT COUNT(*) as count FROM stg_movies_clean", engine)["count"][0]

logging.info(f"Movies raw data count: {raw_count}")
logging.info(f"Movies clean data count: {clean_count}")

if clean_count > raw_count:
    logging.error("Error: Cleaning data is more than raw data")
    raise Exception("Movies row count validation failed")

# checking ratings row count
raw_count = pd.read_sql("SELECT COUNT(*) as count FROM stg_ratings", engine)["count"][0]
clean_count = pd.read_sql("SELECT COUNT(*) as count FROM stg_ratings_clean", engine)["count"][0]

logging.info(f"Ratings raw data count: {raw_count}")
logging.info(f"Ratings clean data count: {clean_count}")

if clean_count > raw_count:
    logging.error("Error: Cleaning data is more than raw data")
    raise Exception("Ratings row count validation failed")
logging.info("Row count validation passed :)")

# checking for orphan ratings
logging.info("Starting Orphan count validation...")

orphan_query = """
    SELECT COUNT(*) as count
    FROM stg_ratings_clean r
    LEFT JOIN stg_movies_clean m
    ON r.movie_id = m.movie_id
    WHERE m.movie_id IS NULL
"""

orphan_count = pd.read_sql(orphan_query, engine)["count"][0]
logging.info(f"Orphan count is {orphan_count}")

if orphan_count != 0:
    logging.error("Ratings without movies exist")
    raise Exception("Orphan count validation failed")
logging.info("Orphan count validation passed :)")

# checking rating range
logging.info("Starting Rating Range validation...")
rating_range_query = """
    SELECT MIN(rating) as min_rating, MAX(rating) as max_rating
    FROM stg_ratings_clean
"""
rating_stats = pd.read_sql(rating_range_query, engine)
min_rating = rating_stats["min_rating"][0]
max_rating = rating_stats["max_rating"][0]

logging.info(f"Min rating: {min_rating}, Max rating: {max_rating}")
if min_rating < 0.5 or max_rating > 5.0:
    logging.error("Ratings out of range exist")
    raise Exception("Rating range validation failed")

logging.info("Rating range validation passed :)")
logging.info("All quality checks passed successfully!")