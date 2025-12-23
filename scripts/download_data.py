import os
import requests
import zipfile

# defining path
url = "https://files.grouplens.org/datasets/movielens/ml-32m.zip"
raw_data_folder = os.path.join("data", "raw")
zip_path = os.path.join(raw_data_folder, "ml-32m.zip")

# create directory if not exists
os.makedirs(raw_data_folder, exist_ok=True) 

# download file if not exists
if not os.path.exists(zip_path):
    print("Downloading data...")
    response = requests.get(url)
    with open(zip_path, "wb") as f:
        f.write(response.content)
    print("Download successfully")
else:
    print("Already downloaded")

# Extracting csv files
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    print("Extracting data...")
    zip_ref.extract("ml-32m/movies.csv", raw_data_folder)
    zip_ref.extract("ml-32m/ratings.csv", raw_data_folder)
    print("Extraction completed")

# Move extracted files to raw_data_folder
os.rename(os.path.join(raw_data_folder, "ml-32m", "movies.csv"), 
          os.path.join(raw_data_folder, "movies.csv"))
os.rename(os.path.join(raw_data_folder, "ml-32m", "ratings.csv"), 
          os.path.join(raw_data_folder, "ratings.csv"))
# delete empty folder
os.rmdir(os.path.join(raw_data_folder, "ml-32m")) 