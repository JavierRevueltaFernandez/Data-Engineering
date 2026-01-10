import os
import logging
import pandas as pd


def build_dim_country(country_meta_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a unified country dimension from country metadata.
    """
    logging.info("Building dim_country")

    dim_country = country_meta_df.copy()
    dim_country.columns = [c.strip() for c in dim_country.columns]

    if "Country Code" not in dim_country.columns:
        raise ValueError("Country metadata is missing 'Country Code' column")

    # Keep one row per country code
    dim_country = dim_country.drop_duplicates(subset=["Country Code"])

    logging.info(f"dim_country shape: {dim_country.shape}")
    logging.info(f"dim_country sample:\n{dim_country.head(5).to_string(index=False)}")
    return dim_country


def build_dim_indicator(gdp_indicator_meta_df: pd.DataFrame, uem_indicator_meta_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build indicator dimension by combining both indicator metadata files.
    Output columns: indicator_code, indicator_name, source_note, source_organization
    """
    logging.info("Building dim_indicator")

    gdp_df = gdp_indicator_meta_df.copy()
    uem_df = uem_indicator_meta_df.copy()

    # Standardize columns
    gdp_df.columns = [c.strip().lower() for c in gdp_df.columns]
    uem_df.columns = [c.strip().lower() for c in uem_df.columns]

    expected_cols = {"indicator_code", "indicator_name", "source_note", "source_organization"}

    for name, df in [("GDP indicator metadata", gdp_df), ("Unemployment indicator metadata", uem_df)]:
        missing = expected_cols - set(df.columns)
        if missing:
            raise ValueError(f"{name} is missing columns: {sorted(missing)}")

    dim_indicator = pd.concat([gdp_df, uem_df], ignore_index=True)
    dim_indicator = dim_indicator.drop_duplicates(subset=["indicator_code"])

    logging.info(f"dim_indicator shape: {dim_indicator.shape}")
    logging.info(f"dim_indicator rows:\n{dim_indicator.to_string(index=False)}")
    return dim_indicator


def build_fact_table(gdp_long_df: pd.DataFrame, uem_long_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a single fact table combining both indicators in long format.
    Output columns: country_code, year, indicator_code, value
    """
    logging.info("Building fact_economic_indicators")

    required_cols = ["Country Code", "year", "Indicator Code", "value"]

    for name, df in [("GDP facts", gdp_long_df), ("Unemployment facts", uem_long_df)]:
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"{name} dataset is missing required columns: {missing}")

    gdp_fact = gdp_long_df[required_cols].copy()
    uem_fact = uem_long_df[required_cols].copy()

    fact_df = pd.concat([gdp_fact, uem_fact], ignore_index=True)

    # Standardize names
    fact_df = fact_df.rename(
        columns={
            "Country Code": "country_code",
            "Indicator Code": "indicator_code"
        }
    )

    # Optional sorting for readability
    fact_df = fact_df.sort_values(["country_code", "indicator_code", "year"]).reset_index(drop=True)

    logging.info(f"fact_economic_indicators shape: {fact_df.shape}")
    logging.info(f"fact_economic_indicators missing values in 'value': {int(fact_df['value'].isnull().sum())}")
    logging.info(f"fact_economic_indicators sample:\n{fact_df.head(10).to_string(index=False)}")

    return fact_df


def save_processed_outputs(
    processed_path: str,
    dim_country: pd.DataFrame,
    dim_indicator: pd.DataFrame,
    fact_df: pd.DataFrame
) -> None:
    """
    Save dimensional model outputs into data/processed.
    """
    logging.info("Saving processed datasets into data/processed")

    os.makedirs(processed_path, exist_ok=True)

    dim_country_file = os.path.join(processed_path, "dim_country.csv")
    dim_indicator_file = os.path.join(processed_path, "dim_indicator.csv")
    fact_file = os.path.join(processed_path, "fact_economic_indicators.csv")

    dim_country.to_csv(dim_country_file, index=False)
    dim_indicator.to_csv(dim_indicator_file, index=False)
    fact_df.to_csv(fact_file, index=False)

    logging.info(f"Saved dim_country to: {dim_country_file}")
    logging.info(f"Saved dim_indicator to: {dim_indicator_file}")
    logging.info(f"Saved fact_economic_indicators to: {fact_file}")
