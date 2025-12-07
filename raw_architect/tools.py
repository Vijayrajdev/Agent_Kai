import io
import os
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound

# Load environment logic
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "data-architect")
PREFIX = os.getenv("LANDING_FOLDER", "landing/")


def check_dataset_exists(dataset_id: str) -> str:
    """
    Checks if a BigQuery dataset exists.
    Args:
        dataset_id: The project.dataset (e.g., 'my_project.raw_zone').
    """
    client = bigquery.Client()
    try:
        client.get_dataset(dataset_id)
        return f"Exists: Dataset '{dataset_id}' is available."
    except NotFound:
        return f"Missing: Dataset '{dataset_id}' does not exist."
    except Exception as e:
        return f"Error checking dataset: {str(e)}"


def create_dataset(dataset_id: str) -> str:
    """
    Creates a new BigQuery dataset (default location: US).
    Args:
        dataset_id: The project.dataset to create.
    """
    client = bigquery.Client()
    try:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = (
            "US"  # You can ask the user for this if needed, defaulting to US for now
        )
        client.create_dataset(dataset, timeout=30)
        return f"✅ Success: Dataset '{dataset_id}' created."
    except Exception as e:
        return f"❌ Failed to create dataset: {str(e)}"


def list_landing_files() -> str:
    """
    Lists all CSV files currently available in the 'landing/' folder of the GCS bucket.
    Returns: A list of filenames.
    """
    try:
        storage_client = storage.Client()
        blobs = storage_client.list_blobs(BUCKET_NAME, prefix=PREFIX)

        # Filter for files only (skip the folder itself)
        files = [
            blob.name.replace(PREFIX, "")
            for blob in blobs
            if not blob.name.endswith("/")
        ]

        if not files:
            return "No files found in the landing folder."
        return f"Files found in gs://{BUCKET_NAME}/{PREFIX}: {', '.join(files)}"
    except Exception as e:
        return f"Error accessing GCS: {str(e)}"


def analyze_gcs_header(file_name: str) -> str:
    """
    Reads the header of a specific file from the landing folder.
    Args:
        file_name: The name of the file (e.g., 'sales.csv').
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(f"{PREFIX}{file_name}")

        # Smart Read: Download only first 2KB
        data_bytes = blob.download_as_bytes(start=0, end=2048)
        data_str = data_bytes.decode("utf-8")

        # Parse header
        df = pd.read_csv(io.StringIO(data_str), nrows=0)
        return f"File '{file_name}' contains columns: {list(df.columns)}"
    except Exception as e:
        return f"Error reading file '{file_name}': {str(e)}"


def create_raw_table(dataset_id: str, table_name: str, ddl: str) -> str:
    """
    Executes the DDL statement to create the table in BigQuery.
    Args:
        dataset_id: The project.dataset (e.g., 'my_project.raw_zone').
        table_name: The target table name (e.g., 'sales_raw').
        ddl: The full CREATE TABLE statement.
    """
    try:
        client = bigquery.Client()

        # Safety Check
        if table_name not in ddl:
            return f"❌ Safety Stop: The generated DDL does not match the requested table name '{table_name}'."

        job = client.query(ddl)
        job.result()
        return f"✅ Success! Table `{dataset_id}.{table_name}` created."
    except Exception as e:
        return f"❌ Failed to create table: {str(e)}"
