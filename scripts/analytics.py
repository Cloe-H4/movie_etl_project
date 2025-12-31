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

# setup logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename=os.path.join("logs", "analytics.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

logging.info(f"{'-'*55}")
# connect to database
load_dotenv()
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
logging.info("Connected to database for analytics operations")

# creating analytics queries
queries = {
    "top_10_movies": """
        SELECT m.title, AVG(r.rating) as avg_rating
        FROM fact_ratings r
        JOIN dim_movies m ON r.movie_key = m.movie_key
        GROUP BY m.title
        ORDER BY avg_rating DESC
        LIMIT 10;
    """,
    "least_10_movies": """
        SELECT m.title, AVG(r.rating) as avg_rating
        FROM fact_ratings r
        JOIN dim_movies m ON r.movie_key = m.movie_key
        GROUP BY m.title
        ORDER BY avg_rating ASC
        LIMIT 10;
    """,
    "top_5_genres": """
        SELECT g.genre as genre_name, COUNT(r.rating) as rating_count
        FROM fact_ratings r
        JOIN bridge_movie_genres bmg ON r.movie_key = bmg.movie_key
        JOIN dim_genres g ON bmg.genre_key = g.genre_key
        GROUP BY g.genre
        ORDER BY rating_count DESC
        LIMIT 5;
    """,
    "least_5_genres": """
        SELECT g.genre as genre_name, COUNT(r.rating) as rating_count
        FROM fact_ratings r
        JOIN bridge_movie_genres bmg ON r.movie_key = bmg.movie_key
        JOIN dim_genres g ON bmg.genre_key = g.genre_key
        GROUP BY g.genre
        ORDER BY rating_count ASC
        LIMIT 5;
    """
}

# execute analytics queries
logging.info("Starting analytics operations")
             
for name, query in queries.items():
    logging.info(f"Executing analytics query: {name}")
    try:
        result = pd.read_sql(query, engine)
        logging.info(f"Query {name} executed successfully")
        
        result.to_csv(
            os.path.join("data", "results", f"{name}.csv"),
            index=False
        )
    except Exception as e:
        logging.error(f"Error executing query {name}: {e}")

logging.info("Analytics operations completed successfully.")