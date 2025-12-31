import os
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# setup logging
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename=os.path.join("logs", "clean.log"),
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

# sql for cleaning data
clean_movies_sql = text("""
DROP TABLE IF EXISTS stg_movies_clean;

CREATE TABLE stg_movies_clean AS
SELECT DISTINCT 
        "movieId" AS movie_id,
        title,
        genres
FROM stg_movies
WHERE "movieId" IS NOT NULL
        AND title IS NOT NULL;
""")

clean_ratings_sql = text("""
DROP TABLE IF EXISTS stg_ratings_clean;

CREATE TABLE stg_ratings_clean AS
SELECT DISTINCT 
        "userId" AS user_id,
        "movieId" AS movie_id,
        rating,
        timestamp
FROM stg_ratings
WHERE "userId" IS NOT NULL
        AND "movieId" IS NOT NULL
        AND rating between 0.5 AND 5.0;
""")

# execute cleaning
logging.info("Starting data cleaning process")

try:
    with engine.begin() as conn:
        logging.info("Creating clean_movies table...")
        conn.execute(clean_movies_sql)
        logging.info("clean_movies table created successfully.")

        logging.info("Creating clean_ratings table...")
        conn.execute(clean_ratings_sql)
        logging.info("clean_ratings table created successfully.")
except Exception as e:
    logging.error(f"Error during data cleaning: {e}")
    raise

logging.info("Data cleaning process completed.")