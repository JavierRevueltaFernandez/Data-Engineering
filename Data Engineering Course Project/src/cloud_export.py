import os
import glob
import logging
from typing import Optional, List

from google.cloud import storage
from google.cloud import bigquery


def upload_processed_to_gcs(
    processed_dir: str,
    bucket_name: str,
    gcs_prefix: str = "processed/",
    include_logs: bool = False,
    logs_dir: Optional[str] = None,
) -> List[str]:
    """
    Upload final pipeline outputs to Google Cloud Storage (GCS).

    Uploads:
    - data/processed/*.csv
    - optionally logs/*.log

    Returns a list of GCS URIs uploaded.
    """
    if not bucket_name:
        raise ValueError("bucket_name must be provided")

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    uploaded: List[str] = []

    # Upload processed CSVs
    csv_paths = sorted(glob.glob(os.path.join(processed_dir, "*.csv")))
    if not csv_paths:
        logging.warning(f"No CSV files found in: {processed_dir}")
        return uploaded

    logging.info(f"GCS export: uploading {len(csv_paths)} CSVs to bucket '{bucket_name}'")

    for local_path in csv_paths:
        filename = os.path.basename(local_path)
        blob_path = f"{gcs_prefix}{filename}"
        blob = bucket.blob(blob_path)
        blob.upload_from_filename(local_path)
        uri = f"gs://{bucket_name}/{blob_path}"
        uploaded.append(uri)
        logging.info(f"Uploaded -> {uri}")

    # Optional: upload logs
    if include_logs:
        if not logs_dir:
            logging.warning("include_logs=True but logs_dir=None; skipping logs upload.")
        else:
            log_paths = sorted(glob.glob(os.path.join(logs_dir, "*.log")))
            logging.info(f"GCS export: uploading {len(log_paths)} logs to bucket '{bucket_name}'")
            for local_path in log_paths:
                filename = os.path.basename(local_path)
                blob_path = f"{gcs_prefix}logs/{filename}"
                blob = bucket.blob(blob_path)
                blob.upload_from_filename(local_path)
                uri = f"gs://{bucket_name}/{blob_path}"
                uploaded.append(uri)
                logging.info(f"Uploaded -> {uri}")

    return uploaded


def load_tables_to_bigquery(
    project_id: str,
    dataset_id: str,
    table_prefix: str,
    dim_country_df,
    dim_indicator_df,
    fact_df,
    location: str = "EU",
) -> None:
    """
    Load the dimensional model into BigQuery (overwrite per run).

    Creates / overwrites:
    - {table_prefix}_dim_country
    - {table_prefix}_dim_indicator
    - {table_prefix}_fact_economic_indicators
    """
    if not project_id or not dataset_id:
        raise ValueError("project_id and dataset_id are required")

    client = bigquery.Client(project=project_id, location=location)

    # Ensure dataset exists
    dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
    dataset_ref.location = location
    try:
        client.get_dataset(dataset_ref)
        logging.info(f"BigQuery dataset exists: {project_id}.{dataset_id}")
    except Exception:
        client.create_dataset(dataset_ref)
        logging.info(f"Created BigQuery dataset: {project_id}.{dataset_id}")

    def _load_df(df, table_name: str):
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        logging.info(f"Loaded {len(df)} rows -> {table_id}")

    _load_df(dim_country_df, f"{table_prefix}_dim_country")
    _load_df(dim_indicator_df, f"{table_prefix}_dim_indicator")
    _load_df(fact_df, f"{table_prefix}_fact_economic_indicators")
