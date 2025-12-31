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

# defining path
url = "https://files.grouplens.org/datasets/movielens/ml-32m.zip"
raw_data_folder = os.path.join("data", "raw")
zip_path = os.path.join(raw_data_folder, "ml-32m.zip")

# create directory if not exists
os.makedirs(raw_data_folder, exist_ok=True) 

# download zip file
logging.info("Starting data download...")

try:
    if os.path.exists(zip_path):
        os.remove(zip_path)
        logging.info("Old zip file removed")

    response = requests.get(url)
    with open(zip_path, "wb") as f:
        f.write(response.content)
except Exception as e:
    logging.error(f"Download failed: {e}")
    raise

logging.info("New zip file downloaded successfully")

# Extracting csv files
logging.info("Extracting data from zip file...")

with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extract("ml-32m/movies.csv", raw_data_folder)
    zip_ref.extract("ml-32m/ratings.csv", raw_data_folder)
    logging.info("Extraction completed")

# Move extracted files to raw_data_folder
if os.path.exists(os.path.join(raw_data_folder, "movies.csv")):
    os.remove(os.path.join(raw_data_folder, "movies.csv"))
if os.path.exists(os.path.join(raw_data_folder, "ratings.csv")):
    os.remove(os.path.join(raw_data_folder, "ratings.csv"))

os.rename(os.path.join(raw_data_folder, "ml-32m", "movies.csv"), 
          os.path.join(raw_data_folder, "movies.csv"))
os.rename(os.path.join(raw_data_folder, "ml-32m", "ratings.csv"), 
          os.path.join(raw_data_folder, "ratings.csv"))
# delete empty folder
os.rmdir(os.path.join(raw_data_folder, "ml-32m")) 

logging.info("Data files are ready in raw data folder")