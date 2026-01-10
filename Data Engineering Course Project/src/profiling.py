import logging
import pandas as pd
from io import StringIO


def log_df_info(df: pd.DataFrame, title: str) -> None:
    """
    Capture df.info() output and write it to logging.
    """
    buffer = StringIO()
    df.info(buf=buffer)
    logging.info(f"{title}\n{buffer.getvalue()}")


def profile_fact_datasets(gdp_df: pd.DataFrame, uem_df: pd.DataFrame) -> tuple[int, int]:
    #--------- Initial profiling (facts) ----------
    logging.info("GDP dataset - first rows")
    logging.info("\n" + gdp_df.head(3).to_string(index=False))

    logging.info("Unemployment dataset - first rows")
    logging.info("\n" + uem_df.head(3).to_string(index=False))

    logging.info("GDP dataset - schema info")
    log_df_info(gdp_df, "GDP dataset - df.info()")

    logging.info("Unemployment dataset - schema info")
    log_df_info(uem_df, "Unemployment dataset - df.info()")

    #--------- Missing values (simple count) ----------
    gdp_missing = int(gdp_df.isnull().sum().sum())
    uem_missing = int(uem_df.isnull().sum().sum())

    logging.info(f"Total missing values in GDP dataset: {gdp_missing}")
    logging.info(f"Total missing values in unemployment dataset: {uem_missing}")

    logging.info("Fact datasets ingestion and profiling finished")

    return gdp_missing, uem_missing


def profile_metadata_datasets(
    gdp_country_meta_df: pd.DataFrame,
    uem_country_meta_df: pd.DataFrame,
    gdp_indicator_meta_df: pd.DataFrame,
    uem_indicator_meta_df: pd.DataFrame
) -> None:
    #--------- Quick profiling for metadata ----------
    logging.info("GDP country metadata - schema info")
    log_df_info(gdp_country_meta_df, "GDP country metadata - df.info()")

    logging.info("Unemployment country metadata - schema info")
    log_df_info(uem_country_meta_df, "Unemployment country metadata - df.info()")

    logging.info("GDP indicator metadata - schema info")
    log_df_info(gdp_indicator_meta_df, "GDP indicator metadata - df.info()")

    logging.info("Unemployment indicator metadata - schema info")
    log_df_info(uem_indicator_meta_df, "Unemployment indicator metadata - df.info()")

    #--------- Shapes (easier than reading full info output) ----------
    logging.info(f"GDP country metadata shape: {gdp_country_meta_df.shape}")
    logging.info(f"Unemployment country metadata shape: {uem_country_meta_df.shape}")
    logging.info(f"GDP indicator metadata shape: {gdp_indicator_meta_df.shape}")
    logging.info(f"Unemployment indicator metadata shape: {uem_indicator_meta_df.shape}")
