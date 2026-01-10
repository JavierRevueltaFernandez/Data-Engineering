import logging
import pandas as pd


def drop_unnamed_columns(df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    """
    Remove extra columns like 'Unnamed: 69' that contain only missing values.
    """
    before_cols = len(df.columns)
    df = df.loc[:, ~df.columns.str.contains(r"^Unnamed", na=False)]
    after_cols = len(df.columns)

    if before_cols != after_cols:
        logging.info(f"{dataset_name} - dropped {before_cols - after_cols} unnamed columns")
    else:
        logging.info(f"{dataset_name} - no unnamed columns to drop")

    return df


def reshape_facts_wide_to_long(df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    """
    Convert World Bank wide format (one column per year) into long format:
    one row per (country, year, indicator).
    """
    id_cols = ["Country Name", "Country Code", "Indicator Name", "Indicator Code"]

    # Basic validation (helps avoid silent bugs)
    missing_id_cols = [c for c in id_cols if c not in df.columns]
    if missing_id_cols:
        raise ValueError(f"{dataset_name} is missing required columns: {missing_id_cols}")

    logging.info(f"{dataset_name} - reshaping from wide to long format")

    long_df = df.melt(
        id_vars=id_cols,
        var_name="year",
        value_name="value"
    )

    # Convert year to numeric; non-year values become NaN (should not happen, but safer)
    long_df["year"] = pd.to_numeric(long_df["year"], errors="coerce")

    # Keep only valid years
    before_rows = len(long_df)
    long_df = long_df.dropna(subset=["year"])
    long_df["year"] = long_df["year"].astype(int)
    after_rows = len(long_df)

    if before_rows != after_rows:
        logging.info(f"{dataset_name} - removed {before_rows - after_rows} rows with invalid year values")

    logging.info(f"{dataset_name} - long format shape: {long_df.shape}")
    logging.info(f"{dataset_name} - sample rows:\n{long_df.head(5).to_string(index=False)}")

    return long_df


def year_coverage_report(long_df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    """
    Compute coverage by year: % of country-year rows that have a non-null value.
    Assumes long format with at least: 'Country Code', 'year', 'value'.
    """
    required_cols = ["Country Code", "year", "value"]
    missing = [c for c in required_cols if c not in long_df.columns]
    if missing:
        raise ValueError(f"{dataset_name} is missing required columns: {missing}")

    logging.info(f"{dataset_name} - computing coverage report by year")

    coverage_df = (
        long_df.groupby("year")
        .agg(
            total_countries=("Country Code", "nunique"),
            non_null_values=("value", lambda s: int(s.notnull().sum()))
        )
        .reset_index()
    )

    # Note: total possible rows per year is number of unique countries in that year.
    # In practice, Country Code may vary slightly across years, but it is good enough for this project.
    coverage_df["coverage_ratio"] = coverage_df["non_null_values"] / coverage_df["total_countries"]

    logging.info(f"{dataset_name} - coverage sample:\n{coverage_df.head(10).to_string(index=False)}")
    return coverage_df


def select_common_year_range(
    gdp_coverage_df: pd.DataFrame,
    uem_coverage_df: pd.DataFrame,
    min_coverage_ratio: float = 0.80
) -> tuple[int, int]:
    """
    Select a common year range where BOTH datasets have at least min_coverage_ratio.
    Returns (start_year, end_year). Raises an error if no overlap is found.
    """
    logging.info(f"Selecting common year range with min coverage ratio = {min_coverage_ratio}")

    gdp_good_years = set(gdp_coverage_df.loc[gdp_coverage_df["coverage_ratio"] >= min_coverage_ratio, "year"].tolist())
    uem_good_years = set(uem_coverage_df.loc[uem_coverage_df["coverage_ratio"] >= min_coverage_ratio, "year"].tolist())

    common_years = sorted(gdp_good_years.intersection(uem_good_years))
    if not common_years:
        raise ValueError("No common years found with the selected coverage threshold.")

    start_year = common_years[0]
    end_year = common_years[-1]

    logging.info(f"Common year range selected: {start_year} to {end_year}")
    return start_year, end_year


def filter_by_year_range(long_df: pd.DataFrame, start_year: int, end_year: int, dataset_name: str) -> pd.DataFrame:
    """
    Filter a long-format dataset to a given inclusive year range.
    """
    before_rows = len(long_df)
    filtered_df = long_df[(long_df["year"] >= start_year) & (long_df["year"] <= end_year)].copy()
    after_rows = len(filtered_df)

    logging.info(f"{dataset_name} - filtered by years {start_year}-{end_year}: {before_rows} -> {after_rows} rows")
    return filtered_df

