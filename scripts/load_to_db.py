import os
import logging
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

# setup logging
log_file_path = os.path.join("logs", "load_to_db.log")
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=log_file_path,
    filemode="w"
)

# connect to database
load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# create file paths
movies_csv_path = os.path.join("data", "raw", "movies.csv")
ratings_csv_path = os.path.join("data", "raw", "ratings.csv")

# loading data using chunks for large files
# loading movies data
chunksize = 50000
first_chunk = True

logging.info("Loading movies data to database...")
for chunk in pd.read_csv(movies_csv_path, chunksize=chunksize):
    try:
        chunk.to_sql("stg_movies", 
                     engine, 
                     if_exists="replace" if first_chunk else "append",
                     index=False,
                     method="multi")
        first_chunk = False
        logging.info(f"loaded {len(chunk)} rows...")
    except Exception as e:
        logging.error(f"Oops, error occurred: {e}")
logging.info("Movies data loaded successfully.")

# loading ratings data
first_chunk = True

logging.info("Loading ratings data to database...")
for chunk in pd.read_csv(ratings_csv_path, chunksize=chunksize):
    try:
        chunk.to_sql("stg_ratings",
                     engine,
                     if_exists="replace" if first_chunk else "append",
                     index=False,
                     method="multi")
        first_chunk = False
        logging.info(f"loaded {len(chunk)} rows...")
    except Exception as e:
        logging.error(f"Oops, error occurred: {e}")

logging.info("Ratings data loaded successfully.")
logging.info("Data loading to database completed successfully.")
