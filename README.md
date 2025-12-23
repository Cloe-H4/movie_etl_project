# MovieLens ELT Automation Pipeline

This project implements an end-to-end ELT data pipeline using the MovieLens 32M dataset.
The pipeline extracts data from CSV files, loads them into a PostgreSQL database,
transforms the data into a star schema, runs analytical queries, and stores results
as CSV files. The workflow is automated using Apache Airflow.
