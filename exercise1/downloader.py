import logging #for logging purposes
import os # for interacting with the operating system (create folders, check paths, remove files)
import requests #for making HTTP requests. Use 'pip install requests' if not already installed
import zipfile #for handling zip files. This is part of the Python standard library


download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

DOWNLOAD_DIR = "downloads"


# Create downloads folder if it does not exist
def verify_download_directory():
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
        print("Folder created")
    else:
        print("Folder already exists")


# Download a file from URL and save it in downloads
def download_file(url):
    try:
        file_name = url.split("/")[-1]  # take the last part of the URL as filename
        file_path = os.path.join(DOWNLOAD_DIR, file_name)

        print(f"Downloading {file_name} ...")
        response = requests.get(url, stream=True, timeout=20)
        response.raise_for_status()  # raise an exception if the request fails

        # Save file in chunks to avoid memory overload
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        print(f"Downloaded: {file_path}")
        return file_path
    except requests.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return None
    

# Unzip file and delete the compressed file
def unzip(filepath):
    if filepath is None:
        return

    try:
        with zipfile.ZipFile(filepath, "r") as zip_ref:
            zip_ref.extractall(DOWNLOAD_DIR)  # extract all files into downloads folder
        os.remove(filepath)  # delete the ZIP file after extraction
        print("File extracted")
    except zipfile.BadZipFile:
        print(f"Corrupted or invalid ZIP file: {filepath}")




def main():
    verify_download_directory()

    for url in download_uris:
        filepath = download_file(url)
        unzip(filepath)

    print("Process completed. Files available in 'downloads' folder.")


if __name__ == "__main__":
    main()



