import logging #for logging purposes
import os
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

def main() -> None:
    response = requests.get("https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip")
    print(response.content)
    pass

if _name_ == "_main_":
    main()



