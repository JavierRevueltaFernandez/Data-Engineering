import logging
import os

from src.cloud_export import upload_processed_to_gcs, load_tables_to_bigquery

from src.config import (
    get_project_root,
    get_raw_data_path,
    get_processed_data_path,
    ENABLE_GCS_EXPORT,
    GCP_BUCKET_NAME,
    GCS_PREFIX,
    INCLUDE_LOGS_IN_GCS,
    ENABLE_BQ_EXPORT,
    BQ_PROJECT_ID,
    BQ_DATASET_ID,
    BQ_TABLE_PREFIX,
    BQ_LOCATION,
)

from src.logging_utils import setup_logging
from src.ingestion import load_fact_datasets, load_metadata_datasets
from src.profiling import profile_fact_datasets, profile_metadata_datasets
from src.preprocessing import (
    drop_unnamed_columns,
    reshape_facts_wide_to_long,
    year_coverage_report,
    select_common_year_range,
    filter_by_year_range,
)
from src.modeling import build_dim_country, build_dim_indicator, build_fact_table, save_processed_outputs
from src.data_quality import validate_and_clean_dimensions, validate_and_clean_fact_table


def main() -> None:
    project_root = get_project_root()
    log_file = setup_logging(project_root, keep_last=10)

    raw_data_path = get_raw_data_path(project_root)
    processed_path = get_processed_data_path(project_root)

    # -------------------- 2) Ingestion --------------------
    logging.info("Starting data ingestion (raw CSV files)")
    gdp_df, uem_df = load_fact_datasets(raw_data_path)
    gdp_country_meta_df, uem_country_meta_df, gdp_indicator_meta_df, uem_indicator_meta_df = load_metadata_datasets(raw_data_path)

    # -------------------- 2) Profiling --------------------
    profile_fact_datasets(gdp_df, uem_df)
    profile_metadata_datasets(
        gdp_country_meta_df,
        uem_country_meta_df,
        gdp_indicator_meta_df,
        uem_indicator_meta_df,
    )

    # -------------------- 3) Structural preprocessing --------------------
    gdp_df = drop_unnamed_columns(gdp_df, "GDP facts dataset")
    uem_df = drop_unnamed_columns(uem_df, "Unemployment facts dataset")

    gdp_country_meta_df = drop_unnamed_columns(gdp_country_meta_df, "GDP country metadata")
    uem_country_meta_df = drop_unnamed_columns(uem_country_meta_df, "Unemployment country metadata")
    gdp_indicator_meta_df = drop_unnamed_columns(gdp_indicator_meta_df, "GDP indicator metadata")
    uem_indicator_meta_df = drop_unnamed_columns(uem_indicator_meta_df, "Unemployment indicator metadata")

    logging.info("Checking if both country metadata files are identical")
    same_shape = gdp_country_meta_df.shape == uem_country_meta_df.shape
    same_columns = list(gdp_country_meta_df.columns) == list(uem_country_meta_df.columns)
    logging.info(f"Country metadata same shape: {same_shape}")
    logging.info(f"Country metadata same columns: {same_columns}")
    if same_shape and same_columns:
        logging.info(f"Country metadata identical content: {gdp_country_meta_df.equals(uem_country_meta_df)}")
    else:
        logging.info("Country metadata files differ in shape or columns, content comparison skipped")

    # Wide -> long
    gdp_long_df = reshape_facts_wide_to_long(gdp_df, "GDP facts dataset")
    uem_long_df = reshape_facts_wide_to_long(uem_df, "Unemployment facts dataset")

    logging.info(f"GDP long dataset missing values: {int(gdp_long_df['value'].isnull().sum())}")
    logging.info(f"Unemployment long dataset missing values: {int(uem_long_df['value'].isnull().sum())}")

    # Coverage-based common year range
    gdp_coverage_df = year_coverage_report(gdp_long_df, "GDP facts dataset")
    uem_coverage_df = year_coverage_report(uem_long_df, "Unemployment facts dataset")

    start_year, end_year = select_common_year_range(gdp_coverage_df, uem_coverage_df, min_coverage_ratio=0.80)

    gdp_long_df = filter_by_year_range(gdp_long_df, start_year, end_year, "GDP facts dataset")
    uem_long_df = filter_by_year_range(uem_long_df, start_year, end_year, "Unemployment facts dataset")

    logging.info(f"Final common year range used in facts: {start_year}-{end_year}")

    # -------------------- 4) Dimensional model --------------------
    dim_country = build_dim_country(gdp_country_meta_df)
    dim_indicator = build_dim_indicator(gdp_indicator_meta_df, uem_indicator_meta_df)
    fact_df = build_fact_table(gdp_long_df, uem_long_df)

    # -------------------- 3.2) Data cleaning + validation (DQ) --------------------
    allowed_indicators = {"NY.GDP.PCAP.CD", "SL.UEM.TOTL.ZS"}

    dim_country_clean, dim_indicator_clean = validate_and_clean_dimensions(dim_country, dim_indicator)

    fact_clean = validate_and_clean_fact_table(
        fact_df=fact_df,
        dim_country=dim_country_clean,
        dim_indicator=dim_indicator_clean,
        year_min=start_year,
        year_max=end_year,
        allowed_indicators=allowed_indicators,
    )

    # -------------------- Save processed outputs --------------------
    save_processed_outputs(processed_path, dim_country_clean, dim_indicator_clean, fact_clean)
    logging.info(f"Processed outputs saved in: {processed_path}")

    # ---------- Cloud export (GCS + BigQuery) ----------
    if ENABLE_GCS_EXPORT:
        if not GCP_BUCKET_NAME.strip():
            raise ValueError("ENABLE_GCS_EXPORT=True but GCP_BUCKET_NAME is empty")

        logs_dir = os.path.join(project_root, "logs")
        uploaded = upload_processed_to_gcs(
            processed_dir=processed_path,
            bucket_name=GCP_BUCKET_NAME.strip(),
            gcs_prefix=GCS_PREFIX.strip(),
            include_logs=INCLUDE_LOGS_IN_GCS,
            logs_dir=logs_dir,
        )
        logging.info(f"GCS export done. Uploaded files: {len(uploaded)}")
    else:
        logging.info("GCS export skipped (ENABLE_GCS_EXPORT=False)")

    if ENABLE_BQ_EXPORT:
        if not BQ_PROJECT_ID.strip():
            raise ValueError("ENABLE_BQ_EXPORT=True but BQ_PROJECT_ID is empty")

        load_tables_to_bigquery(
            project_id=BQ_PROJECT_ID.strip(),
            dataset_id=BQ_DATASET_ID.strip(),
            table_prefix=BQ_TABLE_PREFIX.strip(),
            dim_country_df=dim_country_clean,
            dim_indicator_df=dim_indicator_clean,
            fact_df=fact_clean,
            location=BQ_LOCATION.strip(),
        )
        logging.info("BigQuery export done.")
    else:
        logging.info("BigQuery export skipped (ENABLE_BQ_EXPORT=False)")

    logging.info("Pipeline finished successfully")
    logging.info(f"Log file saved at: {log_file}")


if __name__ == "__main__":
    main()
