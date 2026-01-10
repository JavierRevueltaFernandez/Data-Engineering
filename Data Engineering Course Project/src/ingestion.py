import os
import pandas as pd
import logging


def load_fact_datasets(raw_data_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    #--------- Paths to raw data ----------
    GDP_FOLDER = os.path.join(raw_data_path, "gdp_per_capita")
    UEM_FOLDER = os.path.join(raw_data_path, "unemployment_rate")

    GDP_DATA_FILE = os.path.join(GDP_FOLDER, "data_gdp.csv")
    UEM_DATA_FILE = os.path.join(UEM_FOLDER, "data_uem.csv")

    logging.info("Starting data ingestion (raw CSV files)")

    #--------- Load main datasets (facts) ----------
    try:
        gdp_df = pd.read_csv(GDP_DATA_FILE, skiprows=4)
        logging.info("GDP per capita dataset loaded successfully")
    except FileNotFoundError:
        logging.error(f"File not found: {GDP_DATA_FILE}")
        raise

    try:
        uem_df = pd.read_csv(UEM_DATA_FILE, skiprows=4)
        logging.info("Unemployment rate dataset loaded successfully")
    except FileNotFoundError:
        logging.error(f"File not found: {UEM_DATA_FILE}")
        raise

    return gdp_df, uem_df


def load_metadata_datasets(raw_data_path: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    GDP_FOLDER = os.path.join(raw_data_path, "gdp_per_capita")
    UEM_FOLDER = os.path.join(raw_data_path, "unemployment_rate")

    #--------- Load metadata files ----------
    GDP_COUNTRY_META_FILE = os.path.join(GDP_FOLDER, "metadata_country_gdp.csv")
    UEM_COUNTRY_META_FILE = os.path.join(UEM_FOLDER, "metadata_country_uem.csv")

    GDP_INDICATOR_META_FILE = os.path.join(GDP_FOLDER, "metadata_indicator_gdp.csv")
    UEM_INDICATOR_META_FILE = os.path.join(UEM_FOLDER, "metadata_indicator_uem.csv")

    try:
        gdp_country_meta_df = pd.read_csv(GDP_COUNTRY_META_FILE)
        logging.info("GDP country metadata loaded successfully")
    except FileNotFoundError:
        logging.error(f"File not found: {GDP_COUNTRY_META_FILE}")
        raise

    try:
        uem_country_meta_df = pd.read_csv(UEM_COUNTRY_META_FILE)
        logging.info("Unemployment country metadata loaded successfully")
    except FileNotFoundError:
        logging.error(f"File not found: {UEM_COUNTRY_META_FILE}")
        raise

    try:
        gdp_indicator_meta_df = pd.read_csv(GDP_INDICATOR_META_FILE)
        logging.info("GDP indicator metadata loaded successfully")
    except FileNotFoundError:
        logging.error(f"File not found: {GDP_INDICATOR_META_FILE}")
        raise

    try:
        uem_indicator_meta_df = pd.read_csv(UEM_INDICATOR_META_FILE)
        logging.info("Unemployment indicator metadata loaded successfully")
    except FileNotFoundError:
        logging.error(f"File not found: {UEM_INDICATOR_META_FILE}")
        raise

    return gdp_country_meta_df, uem_country_meta_df, gdp_indicator_meta_df, uem_indicator_meta_df
