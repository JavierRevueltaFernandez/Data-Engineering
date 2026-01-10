import os


def get_project_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_raw_data_path(project_root: str) -> str:
    return os.path.join(project_root, "data", "raw")


def get_processed_data_path(project_root: str) -> str:
    return os.path.join(project_root, "data", "processed")


# -------------------- Cloud export config --------------------
# Default: disabled
ENABLE_GCS_EXPORT = False
ENABLE_BQ_EXPORT = True

#GCS
GCP_BUCKET_NAME = "wb-economic-pipeline-jrevuelta-001"
GCS_PREFIX = "processed/"  # folder inside the bucket
INCLUDE_LOGS_IN_GCS = False

#BigQuery
BQ_PROJECT_ID = "wb-economic-pipeline"
BQ_DATASET_ID = "economic_indicators"
BQ_TABLE_PREFIX = "wb"
BQ_LOCATION = "EU"
