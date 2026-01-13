import os
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# =========================
# AIRFLOW PATH CONFIG
# =========================
AIRFLOW_HOME = "/opt/airflow"
LOG_DIR = os.path.join(AIRFLOW_HOME, "logs")

os.makedirs(LOG_DIR, exist_ok=True)

# =========================
# LOGGING SETUP
# =========================
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "warehouse.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

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

logging.info("Connected to database for warehouse operations")

# =========================
# WAREHOUSE SQL DEFINITIONS
# =========================
drop_tables_sql = text("""
DROP TABLE IF EXISTS dim_movies;
DROP TABLE IF EXISTS dim_genres;
DROP TABLE IF EXISTS bridge_movie_genres;
DROP TABLE IF EXISTS fact_ratings;
""")

dim_movies_sql = text("""
CREATE TABLE dim_movies (
    movie_key INT PRIMARY KEY,
    movie_id INT NOT NULL,
    title TEXT NOT NULL
);

INSERT INTO dim_movies
SELECT 
    ROW_NUMBER() OVER (ORDER BY movie_id) AS movie_key,
    movie_id,
    title
FROM stg_movies_clean;
""")

dim_genres_sql = text("""
CREATE TABLE dim_genres (
    genre_key INT PRIMARY KEY,
    genre TEXT NOT NULL
);

INSERT INTO dim_genres
SELECT 
    ROW_NUMBER() OVER (ORDER BY genre) AS genre_key,
    genre
FROM (
    SELECT DISTINCT UNNEST(string_to_array(genres, '|')) AS genre
    FROM stg_movies_clean
) AS g;
""")

bridge_movie_genres_sql = text("""
CREATE TABLE bridge_movie_genres (
    movie_key INT NOT NULL,
    genre_key INT NOT NULL,
    PRIMARY KEY (movie_key, genre_key)
);

INSERT INTO bridge_movie_genres
SELECT 
    m.movie_key,
    g.genre_key
FROM stg_movies_clean cm
JOIN dim_movies m 
    ON cm.movie_id = m.movie_id
JOIN dim_genres g 
    ON g.genre = ANY(string_to_array(cm.genres, '|'));
""")

fact_ratings_sql = text("""
CREATE TABLE fact_ratings (
    rating_key INT PRIMARY KEY,
    movie_key INT NOT NULL,
    user_id INT NOT NULL,
    rating NUMERIC(2,1) NOT NULL,
    rating_timestamp TIMESTAMP NOT NULL
);

INSERT INTO fact_ratings
SELECT 
    ROW_NUMBER() OVER (ORDER BY r.timestamp) AS rating_key,
    m.movie_key,
    r.user_id,
    r.rating,
    to_timestamp(r.timestamp) AS rating_timestamp
FROM stg_ratings_clean r
JOIN dim_movies m 
    ON r.movie_id = m.movie_id;
""")

index_sql = text("""
CREATE INDEX idx_fact_ratings_movie_key 
    ON fact_ratings(movie_key);

CREATE INDEX idx_bridge_genre_key 
    ON bridge_movie_genres(genre_key);
""")

# =========================
# EXECUTE WAREHOUSE CREATION
# =========================
logging.info("Starting warehouse creation.")

try:
    with engine.begin() as conn:
        logging.info("Dropping existing warehouse tables if any exist...")
        conn.execute(drop_tables_sql)

        logging.info("Creating dim_movies...")
        conn.execute(dim_movies_sql)

        logging.info("Creating dim_genres...")
        conn.execute(dim_genres_sql)

        logging.info("Creating bridge_movie_genres...")
        conn.execute(bridge_movie_genres_sql)

        logging.info("Creating fact_ratings...")
        conn.execute(fact_ratings_sql)

        logging.info("Creating indexes...")
        conn.execute(index_sql)

except Exception as e:
    logging.error(f"Warehouse creation failed: {e}")
    raise

logging.info("Warehouse creation completed successfully.")
