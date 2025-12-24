# MovieLens ELT Automation Pipeline

This project implements an end-to-end ELT data pipeline using the MovieLens 32M dataset.
The pipeline extracts data from CSV files, loads them into a PostgreSQL database,
transforms the data into a star schema, runs analytical queries, and stores results
as CSV files. The workflow is automated using Apache Airflow.


## Dataset
- Source: MovieLens 32M Dataset
- URL: https://grouplens.org/datasets/movielens/32m/
- Files used:
  - `movies.csv`
  - `ratings.csv`


## Technology Stack
- Python
- PostgreSQL
- SQLAlchemy
- Pandas
- Apache Airflow
- Git & GitHub


## Data Architecture

**ELT Flow:**

1. Extract  
   - Download the MovieLens dataset as a ZIP file
   - Extract `movies.csv` and `ratings.csv` into the raw data folder

2. Load  
   - Load raw CSV files into PostgreSQL staging tables

3. Transform  
   - Clean and validate data
   - Transform staging tables into a star schema
     - Fact table: ratings
     - Dimension tables: movies, genres, time

4. Analytics  
   - Top 10 movies by average rating
   - Least 10 movies by average rating
   - Top 5 genres by number of ratings
   - Least 5 genres by number of ratings

5. Output  
   - Store analytical query results as CSV files