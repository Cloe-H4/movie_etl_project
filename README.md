# Automated MovieLens Data Pipeline with PostgreSQL and Airflow

This project implements an end-to-end ELT data pipeline using the MovieLens 32M dataset.
The pipeline automates data ingestion, cleaning, validation, transformation into a data warehouse schema, analytical querying, and scheduling using Apache Airflow.



## 🎯 Objectives
- Download and ingest large CSV datasets programmatically
- Load raw data into PostgreSQL staging tables
- Clean and validate data to ensure quality and integrity
- Transform data into a star-schema data warehouse
- Run analytical SQL queries and export results as CSV files
- Schedule and orchestrate the pipeline using Apache Airflow
- Apply logging, modular coding, and error handling throughout



## 📦 Dataset
- **Source:** MovieLens 32M Dataset  
- **URL:** https://grouplens.org/datasets/movielens/32m/  
- **Files used:**
  - `movies.csv`
  - `ratings.csv`



## 🗄️ Database Schema

### Dimension Tables
- `dim_movies` – movie metadata  
- `dim_genres` – distinct movie genres  

### Bridge Table
- `bridge_movie_genres` – resolves many-to-many relationship between movies and genres  

### Fact Table
- `fact_ratings` – user ratings with timestamps  

This star schema enables efficient analytical queries and aggregation.



## 🛠️ Technologies Used
- Python – orchestration and automation
- PostgreSQL – relational database
- SQLAlchemy – database connectivity
- Pandas – query execution and CSV export
- Apache Airflow – scheduling and orchestration
- Logging module – pipeline observability
- Git & GitHub – version control



## 🧱 Data Architecture

### ELT Flow
1. **Extract**
   - Download the MovieLens dataset as a ZIP file
   - Extract `movies.csv` and `ratings.csv` into the raw data folder

2. **Load**
   - Load raw CSV files into PostgreSQL staging tables

3. **Transform**
   - Clean and validate data
   - Transform staging tables into a star schema:
     - Fact table: ratings
     - Dimension tables: movies, genres

4. **Analytics**
   - Top 10 movies by average rating
   - Least 10 movies by average rating
   - Top 5 genres by number of ratings
   - Least 5 genres by number of ratings

5. **Output**
   - Store analytical query results as CSV files



## ✅ Data Quality & Validation
The pipeline enforces multiple data quality checks:
- Row count sanity checks
- Referential integrity between ratings and movies
- Valid rating range enforcement
- Duplicate removal

If any validation fails, the pipeline stops automatically and logs the error.



## 📊 Analytics Performed
The following analytics are generated daily and saved as CSV files:
- Top 10 movies by average rating
- Least 10 movies by average rating
- Top 5 genres by number of ratings
- Least 5 genres by number of ratings



## ⏰ Scheduling with Airflow
The pipeline is scheduled using Apache Airflow.

- **Schedule:** Daily at 12:00 PM
- **Execution Flow:**  
  `download → extract → load → clean → quality_checks → warehouse → analytics`

Failures stop downstream tasks automatically.



## 👤 Author
**Nchedochukwu C. Bede**
