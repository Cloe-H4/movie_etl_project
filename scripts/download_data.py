import os
import requests
import zipfile
import logging

# setup logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename=os.path.join("logs", "download_data.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# defining paths
url = "https://files.grouplens.org/datasets/movielens/ml-32m.zip"
raw_data_folder = os.path.join("data", "raw")
zip_path = os.path.join(raw_data_folder, "ml-32m.zip")
movies_csv_path = os.path.join(raw_data_folder, "movies.csv")
ratings_csv_path = os.path.join(raw_data_folder, "ratings.csv")
extracted_folder = os.path.join(raw_data_folder, "ml-32m")

# create directory if not exists
os.makedirs(raw_data_folder, exist_ok=True) 

# download zip file if it doesn't exist
if os.path.exists(zip_path):
    logging.info("Zip file already exists. Skipping download.")
else:
    logging.info("Starting data download...")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logging.info("Download completed successfully.")
    except Exception as e:
        logging.error(f"Download failed: {e}")
        raise

# extract files if they don't already exist
if os.path.exists(movies_csv_path) and os.path.exists(ratings_csv_path):
    logging.info("CSV files already exist. Skipping extraction.")
else:
    logging.info("Extracting data from zip file...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extract("ml-32m/movies.csv", raw_data_folder)
        zip_ref.extract("ml-32m/ratings.csv", raw_data_folder)
    logging.info("Extraction completed.")

    # move extracted files to raw_data_folder
    if os.path.exists(movies_csv_path):
        os.remove(movies_csv_path)
    if os.path.exists(ratings_csv_path):
        os.remove(ratings_csv_path)

    os.rename(os.path.join(extracted_folder, "movies.csv"), movies_csv_path)
    os.rename(os.path.join(extracted_folder, "ratings.csv"), ratings_csv_path)

    # delete empty extracted folder
    if os.path.exists(extracted_folder):
        os.rmdir(extracted_folder)

logging.info("Data files are ready in raw data folder")
