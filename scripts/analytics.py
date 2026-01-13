"""
This script runs analytical queries on the data warehouse and exports results as CSV files.
Analytics include:
- Top 10 movies by average rating
- Least 10 movies by average rating
- Top 5 genres by number of ratings
- Least 5 genres by number of ratings
"""

import os
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv
import pandas as pd

# =========================
# AIRFLOW PATH CONFIG
# =========================
AIRFLOW_HOME = "/opt/airflow"
LOG_DIR = os.path.join(AIRFLOW_HOME, "logs")
RESULTS_DIR = os.path.join(AIRFLOW_HOME, "data", "results")

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)

# =========================
# LOGGING SETUP
# =========================
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "analytics.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

logging.info(f"{'-'*55}")

# =========================
# CONNECT TO DATABASE
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

logging.info("Connected to database for analytics operations")

# =========================
# ANALYTICS QUERIES
# =========================
queries = {
    "top_10_movies": """
        SELECT m.title, AVG(r.rating) AS avg_rating
        FROM fact_ratings r
        JOIN dim_movies m ON r.movie_key = m.movie_key
        GROUP BY m.title
        ORDER BY avg_rating DESC
        LIMIT 10;
    """,
    "least_10_movies": """
        SELECT m.title, AVG(r.rating) AS avg_rating
        FROM fact_ratings r
        JOIN dim_movies m ON r.movie_key = m.movie_key
        GROUP BY m.title
        ORDER BY avg_rating ASC
        LIMIT 10;
    """,
    "top_5_genres": """
        SELECT g.genre AS genre_name, COUNT(r.rating) AS rating_count
        FROM fact_ratings r
        JOIN bridge_movie_genres bmg ON r.movie_key = bmg.movie_key
        JOIN dim_genres g ON bmg.genre_key = g.genre_key
        GROUP BY g.genre
        ORDER BY rating_count DESC
        LIMIT 5;
    """,
    "least_5_genres": """
        SELECT g.genre AS genre_name, COUNT(r.rating) AS rating_count
        FROM fact_ratings r
        JOIN bridge_movie_genres bmg ON r.movie_key = bmg.movie_key
        JOIN dim_genres g ON bmg.genre_key = g.genre_key
        GROUP BY g.genre
        ORDER BY rating_count ASC
        LIMIT 5;
    """
}

# =========================
# EXECUTE ANALYTICS
# =========================
logging.info("Starting analytics operations")

for name, query in queries.items():
    logging.info(f"Executing analytics query: {name}")

    try:
        result = pd.read_sql(query, engine)
        logging.info(f"Query {name} executed successfully")

        output_path = os.path.join(RESULTS_DIR, f"{name}.csv")
        result.to_csv(output_path, index=False)

        logging.info(f"Results written to {output_path}")

    except Exception as e:
        logging.error(f"Error executing query {name}: {e}")
        raise

logging.info("Analytics operations completed successfully.")