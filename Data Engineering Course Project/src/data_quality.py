import logging
from typing import Tuple, Set, List
import pandas as pd


def _log_and_return_examples(df: pd.DataFrame, mask: pd.Series, cols: List[str], n: int = 5) -> str:
    """Utility to format a few example rows for logging."""
    examples = df.loc[mask, cols].head(n)
    if examples.empty:
        return ""
    return "\n" + examples.to_string(index=False)


def validate_and_clean_dimensions(
    dim_country: pd.DataFrame,
    dim_indicator: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Basic dimension cleaning:
    - Drop duplicates by primary key (keep first, log count)
    - Drop rows with null primary key (log count)
    - Trim whitespace in primary keys (defensive, avoids join mismatches)
    """
    # ---- dim_country ----
    if "Country Code" not in dim_country.columns:
        raise ValueError("dim_country must contain 'Country Code' column")

    dim_country = dim_country.copy()

    # Defensive: strip spaces in PK
    dim_country["Country Code"] = dim_country["Country Code"].astype(str).str.strip()
    dim_country.loc[dim_country["Country Code"].isin(["", "nan", "None"]), "Country Code"] = pd.NA

    before = len(dim_country)
    dim_country = dim_country.dropna(subset=["Country Code"])
    dropped_nulls = before - len(dim_country)
    if dropped_nulls:
        logging.warning(f"dim_country - dropped {dropped_nulls} rows with null/empty Country Code")

    dup_mask = dim_country.duplicated(subset=["Country Code"], keep="first")
    dup_count = int(dup_mask.sum())
    if dup_count:
        logging.warning(
            f"dim_country - found {dup_count} duplicate Country Code values; keeping first occurrence"
            + _log_and_return_examples(dim_country, dup_mask, ["Country Code"])
        )
        dim_country = dim_country.loc[~dup_mask].copy()

    # ---- dim_indicator ----
    if "indicator_code" not in dim_indicator.columns:
        raise ValueError("dim_indicator must contain 'indicator_code' column")

    dim_indicator = dim_indicator.copy()

    # Defensive: strip spaces in PK
    dim_indicator["indicator_code"] = dim_indicator["indicator_code"].astype(str).str.strip()
    dim_indicator.loc[dim_indicator["indicator_code"].isin(["", "nan", "None"]), "indicator_code"] = pd.NA

    before = len(dim_indicator)
    dim_indicator = dim_indicator.dropna(subset=["indicator_code"])
    dropped_nulls = before - len(dim_indicator)
    if dropped_nulls:
        logging.warning(f"dim_indicator - dropped {dropped_nulls} rows with null/empty indicator_code")

    dup_mask = dim_indicator.duplicated(subset=["indicator_code"], keep="first")
    dup_count = int(dup_mask.sum())
    if dup_count:
        logging.warning(
            f"dim_indicator - found {dup_count} duplicate indicator_code values; keeping first occurrence"
            + _log_and_return_examples(dim_indicator, dup_mask, ["indicator_code"])
        )
        dim_indicator = dim_indicator.loc[~dup_mask].copy()

    return dim_country, dim_indicator


def validate_and_clean_fact_table(
    fact_df: pd.DataFrame,
    dim_country: pd.DataFrame,
    dim_indicator: pd.DataFrame,
    year_min: int,
    year_max: int,
    allowed_indicators: Set[str],
) -> pd.DataFrame:
    """
    Data Quality rules for the fact table.

    We APPLY minimal, defensible corrections:

    1) Trim whitespace in keys (country_code, indicator_code) to avoid join mismatches
    2) Drop rows with null keys: country_code, year, indicator_code
    3) Coerce year to numeric; drop rows where year cannot be parsed
    4) Normalize year to annual granularity (floor decimals, then cast to int)
    5) Drop rows with year outside [year_min, year_max]
    6) Drop rows with indicator_code not in allowed_indicators
    7) Drop duplicates by (country_code, year, indicator_code)
    8) Referential integrity:
       - If country_code not present in dim_country -> drop row
       - If indicator_code not present in dim_indicator -> drop row
    9) Domain rules (we do NOT drop the row, we nullify value):
       - unemployment must be in [0, 100] when indicator_code == 'SL.UEM.TOTL.ZS'
       - GDP per capita must be > 0 when indicator_code == 'NY.GDP.PCAP.CD'
    """
    required_cols = {"country_code", "year", "indicator_code", "value"}
    missing = required_cols - set(fact_df.columns)
    if missing:
        raise ValueError(f"fact_economic_indicators is missing required columns: {sorted(missing)}")

    df = fact_df.copy()
    start_rows = len(df)

    # 1) Strip whitespace on keys (defensive)
    df["country_code"] = df["country_code"].astype(str).str.strip()
    df["indicator_code"] = df["indicator_code"].astype(str).str.strip()

    # Convert empty-like strings to NA
    df.loc[df["country_code"].isin(["", "nan", "None"]), "country_code"] = pd.NA
    df.loc[df["indicator_code"].isin(["", "nan", "None"]), "indicator_code"] = pd.NA

    # 2) Drop rows with null keys
    key_cols = ["country_code", "year", "indicator_code"]
    null_key_mask = df[key_cols].isnull().any(axis=1)
    null_key_count = int(null_key_mask.sum())
    if null_key_count:
        logging.warning(
            f"fact_economic_indicators - dropping {null_key_count} rows with null keys"
            + _log_and_return_examples(df, null_key_mask, ["country_code", "year", "indicator_code", "value"])
        )
        df = df.loc[~null_key_mask].copy()

    # 3) Coerce year to numeric; drop unparseable
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    bad_year_mask = df["year"].isnull()
    bad_year_count = int(bad_year_mask.sum())
    if bad_year_count:
        logging.warning(
            f"fact_economic_indicators - dropping {bad_year_count} rows with non-numeric year"
            + _log_and_return_examples(df, bad_year_mask, ["country_code", "year", "indicator_code", "value"])
        )
        df = df.loc[~bad_year_mask].copy()

    # 4) Normalize year to annual granularity (floor decimals)
    # Decimal years (e.g. 1993.5) are floored to the corresponding calendar year
    df["year"] = df["year"].astype(float).apply(lambda y: int(y))

    # 5) Drop out-of-range years
    out_range_mask = ~df["year"].between(year_min, year_max)
    out_range_count = int(out_range_mask.sum())
    if out_range_count:
        logging.warning(
            f"fact_economic_indicators - dropping {out_range_count} rows with year outside {year_min}-{year_max}"
            + _log_and_return_examples(df, out_range_mask, ["country_code", "year", "indicator_code", "value"])
        )
        df = df.loc[~out_range_mask].copy()

    # 6) Drop invalid indicator codes (we only want the chosen project indicators)
    invalid_ind_mask = ~df["indicator_code"].isin(list(allowed_indicators))
    invalid_ind_count = int(invalid_ind_mask.sum())
    if invalid_ind_count:
        logging.warning(
            f"fact_economic_indicators - dropping {invalid_ind_count} rows with non-selected indicator_code"
            + _log_and_return_examples(df, invalid_ind_mask, ["country_code", "year", "indicator_code", "value"])
        )
        df = df.loc[~invalid_ind_mask].copy()

    # 7) Drop duplicates by composite key
    dup_mask = df.duplicated(subset=["country_code", "year", "indicator_code"], keep="first")
    dup_count = int(dup_mask.sum())
    if dup_count:
        logging.warning(
            f"fact_economic_indicators - found {dup_count} duplicate (country_code, year, indicator_code); keeping first"
            + _log_and_return_examples(df, dup_mask, ["country_code", "year", "indicator_code", "value"])
        )
        df = df.loc[~dup_mask].copy()

    # 8) Referential integrity -> drop rows not found in dimensions
    # Use stripped string sets to avoid false mismatches
    country_set = set(dim_country["Country Code"].dropna().astype(str).str.strip().unique())
    indicator_set = set(dim_indicator["indicator_code"].dropna().astype(str).str.strip().unique())

    missing_country_mask = ~df["country_code"].astype(str).isin(country_set)
    missing_country_count = int(missing_country_mask.sum())
    if missing_country_count:
        logging.warning(
            f"fact_economic_indicators - dropping {missing_country_count} rows with country_code not in dim_country"
            + _log_and_return_examples(df, missing_country_mask, ["country_code", "year", "indicator_code", "value"])
        )
        df = df.loc[~missing_country_mask].copy()

    missing_indicator_mask = ~df["indicator_code"].astype(str).isin(indicator_set)
    missing_indicator_count = int(missing_indicator_mask.sum())
    if missing_indicator_count:
        logging.warning(
            f"fact_economic_indicators - dropping {missing_indicator_count} rows with indicator_code not in dim_indicator"
            + _log_and_return_examples(df, missing_indicator_mask, ["country_code", "year", "indicator_code", "value"])
        )
        df = df.loc[~missing_indicator_mask].copy()

    # 9) Domain rules -> set invalid values to NaN (do not drop rows)
    # Unemployment must be within [0, 100]
    unemp_code = "SL.UEM.TOTL.ZS"
    unemp_mask = df["indicator_code"] == unemp_code
    invalid_unemp_mask = unemp_mask & df["value"].notnull() & ((df["value"] < 0) | (df["value"] > 100))
    invalid_unemp_count = int(invalid_unemp_mask.sum())
    if invalid_unemp_count:
        logging.warning(
            f"fact_economic_indicators - setting {invalid_unemp_count} invalid unemployment values to NaN (outside [0,100])"
            + _log_and_return_examples(df, invalid_unemp_mask, ["country_code", "year", "indicator_code", "value"])
        )
        df.loc[invalid_unemp_mask, "value"] = pd.NA

    # GDP per capita must be > 0
    gdp_code = "NY.GDP.PCAP.CD"
    gdp_mask = df["indicator_code"] == gdp_code
    invalid_gdp_mask = gdp_mask & df["value"].notnull() & (df["value"] <= 0)
    invalid_gdp_count = int(invalid_gdp_mask.sum())
    if invalid_gdp_count:
        logging.warning(
            f"fact_economic_indicators - setting {invalid_gdp_count} non-positive GDP per capita values to NaN (<= 0)"
            + _log_and_return_examples(df, invalid_gdp_mask, ["country_code", "year", "indicator_code", "value"])
        )
        df.loc[invalid_gdp_mask, "value"] = pd.NA

    end_rows = len(df)
    logging.info(f"fact_economic_indicators - data quality cleaning summary: {start_rows} -> {end_rows} rows")
    logging.info(f"fact_economic_indicators - missing values in 'value' after DQ: {int(df['value'].isnull().sum())}")

    return df
