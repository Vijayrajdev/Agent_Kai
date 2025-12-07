"""
raw_architect/tools.py

This module contains the capability functions (tools) for the Data Architect Agent.
These functions interact with Google Cloud services (GCS, BigQuery) to perform
discovery, analysis, and execution tasks.

Dependencies:
    - google-cloud-bigquery
    - google-cloud-storage
    - pandas
"""

import io
import os
import time
import json
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from google.api_core.exceptions import NotFound

# --- Configuration Constants ---
# Defaults are provided for safe local testing fallback
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "data-architect")
LANDING_PREFIX = os.getenv("LANDING_FOLDER", "landing/")


# ==============================================================================
# GROUP 1: DISCOVERY & NAVIGATION
# ==============================================================================


def list_landing_files() -> str:
    """
    Lists CSV files currently available in the GCS landing folder.

    Returns:
        str: A comma-separated list of filenames or a 'No files' message.
    """
    try:
        storage_client = storage.Client()
        blobs = storage_client.list_blobs(BUCKET_NAME, prefix=LANDING_PREFIX)

        # Filter: Remove the folder itself and keep only files
        files = [
            blob.name.replace(LANDING_PREFIX, "")
            for blob in blobs
            if not blob.name.endswith("/")
        ]

        if not files:
            return "No files found in the landing folder."
        return f"Files found in gs://{BUCKET_NAME}/{LANDING_PREFIX}: {', '.join(files)}"
    except Exception as e:
        return f"Error accessing GCS: {str(e)}"


def list_datasets(project_id: str) -> str:
    """
    Lists all BigQuery datasets in the specified project.

    Args:
        project_id (str): The Google Cloud Project ID.

    Returns:
        str: A list of dataset IDs.
    """
    try:
        client = bigquery.Client(project=project_id)
        datasets = list(client.list_datasets())

        if not datasets:
            return "No datasets found in this project."
        return f"Datasets: {', '.join([d.dataset_id for d in datasets])}"
    except Exception as e:
        return f"Error listing datasets: {str(e)}"


def list_tables(dataset_id: str) -> str:
    """
    Lists all tables within a specific BigQuery dataset.

    Args:
        dataset_id (str): The Dataset ID (e.g., 'raw_zone').

    Returns:
        str: A list of table IDs.
    """
    try:
        client = bigquery.Client()
        tables = list(client.list_tables(dataset_id))

        if not tables:
            return f"No tables found in dataset '{dataset_id}'."
        return f"Tables: {', '.join([t.table_id for t in tables])}"
    except Exception as e:
        return f"Error listing tables: {str(e)}"


# ==============================================================================
# GROUP 2: ANALYSIS & DESIGN
# ==============================================================================


def analyze_gcs_header(file_name: str) -> str:
    """
    Reads the header of a CSV file from GCS to understand its schema.

    Optimization:
        Downloads only the first 2KB of the file to avoid loading large datasets.

    Args:
        file_name (str): The name of the file in the landing folder.

    Returns:
        str: A list of column names found in the file.
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(f"{LANDING_PREFIX}{file_name}")

        # Smart Read: Fetch only first 2KB
        data_bytes = blob.download_as_bytes(start=0, end=2048)
        data_str = data_bytes.decode("utf-8")

        # Parse using pandas (reading only the header line)
        df = pd.read_csv(io.StringIO(data_str), nrows=0)
        return f"File '{file_name}' contains columns: {list(df.columns)}"
    except Exception as e:
        return f"Error reading file '{file_name}': {str(e)}"


def check_dataset_exists(dataset_id: str) -> str:
    """
    Verifies if a BigQuery dataset exists.

    Args:
        dataset_id (str): The Dataset ID to check.

    Returns:
        str: 'Exists' or 'Missing'.
    """
    client = bigquery.Client()
    try:
        client.get_dataset(dataset_id)
        return "Exists: Dataset is available."
    except NotFound:
        return f"Missing: Dataset '{dataset_id}' does not exist."
    except Exception as e:
        return f"Error checking dataset: {str(e)}"


# ==============================================================================
# GROUP 3: EXECUTION & MANAGEMENT
# ==============================================================================


def create_dataset(dataset_id: str) -> str:
    """
    Creates a new BigQuery dataset (defaults to US multi-region).

    Args:
        dataset_id (str): The Dataset ID to create.

    Returns:
        str: Success or error message.
    """
    client = bigquery.Client()
    try:
        ds = bigquery.Dataset(dataset_id)
        ds.location = "US"
        client.create_dataset(ds, timeout=30)
        return f"✅ Success: Dataset '{dataset_id}' created."
    except Exception as e:
        return f"❌ Failed to create dataset: {str(e)}"


def create_raw_table(dataset_id: str, table_name: str, ddl: str) -> str:
    """
    Executes a DDL statement to create a table in BigQuery.

    Args:
        dataset_id (str): The target dataset.
        table_name (str): The target table name (for validation).
        ddl (str): The full CREATE TABLE SQL statement.

    Returns:
        str: Success or error message.
    """
    try:
        client = bigquery.Client()

        # Validation: Ensure DDL matches the intent
        if table_name not in ddl:
            return f"❌ Safety Stop: DDL generated does not match target table '{table_name}'."

        job = client.query(ddl)
        job.result()
        return f"✅ Success! Table `{dataset_id}.{table_name}` created successfully."
    except Exception as e:
        return f"❌ Failed to create table: {str(e)}"


def drop_table(dataset_id: str, table_name: str) -> str:
    """
    Deletes a table from BigQuery.

    WARNING: This is a destructive action.

    Args:
        dataset_id (str): The dataset containing the table.
        table_name (str): The table to delete.

    Returns:
        str: Success or error message.
    """
    try:
        client = bigquery.Client()
        table_ref = f"{client.project}.{dataset_id}.{table_name}"
        client.delete_table(table_ref)
        return f"✅ Table {table_name} has been deleted."
    except Exception as e:
        return f"❌ Failed to delete table: {str(e)}"


def delete_dataset(dataset_id: str, delete_contents: bool = False) -> str:
    """
    Deletes a dataset.

    Args:
        dataset_id: The dataset to delete.
        delete_contents: If True, deletes all tables inside (Nuclear Option).
    """
    client = bigquery.Client()
    try:
        # delete_contents=True is required if the dataset has tables
        client.delete_dataset(
            dataset_id, delete_contents=delete_contents, not_found_ok=True
        )
        return (
            f"✅ Dataset '{dataset_id}' deleted (Contents Deleted: {delete_contents})."
        )
    except Exception as e:
        return f"❌ Failed to delete dataset: {e}"


def run_query(query: str) -> str:
    """
    Executes a general SQL query.
    Used primarily for backup/restore operations (INSERT INTO ... SELECT).

    Args:
        query (str): The SQL query to execute.

    Returns:
        str: Success or error message.
    """
    try:
        client = bigquery.Client()
        job = client.query(query)
        job.result()
        return "✅ Query executed successfully."
    except Exception as e:
        return f"❌ Query execution failed: {str(e)}"


# ==============================================================================
# GROUP 4: SAFETY & ARTIFACTS
# ==============================================================================


def generate_artifacts(table_name: str, ddl_content: str, json_schema: str) -> str:
    """
    Infrastructure-as-Code (IaC) Mode.
    Writes the DDL and JSON Schema to GCS instead of executing them.

    Locations:
        - gs://BUCKET/table_generation/DDL/{table_name}.sql
        - gs://BUCKET/table_generation/json/{table_name}.json

    Args:
        table_name (str): The name of the table.
        ddl_content (str): The SQL Create statement.
        json_schema (str): The BigQuery JSON schema representation.

    Returns:
        str: Success message with GCS paths.
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)

        # 1. Write DDL File
        ddl_blob = bucket.blob(f"table_generation/DDL/{table_name}.sql")
        ddl_blob.upload_from_string(ddl_content)

        # 2. Write JSON Schema File
        json_blob = bucket.blob(f"table_generation/json/{table_name}.json")
        json_blob.upload_from_string(json_schema)

        return f"✅ Artifacts generated successfully in gs://{BUCKET_NAME}/table_generation/"
    except Exception as e:
        return f"❌ Failed to generate artifacts: {str(e)}"


def export_table_backup(dataset_id: str, table_name: str) -> str:
    """
    The 'Nuclear Option' Backup.
    Exports the entire table to GCS CSV before deletion.

    Path:
        gs://BUCKET/backup/bq_table/{dataset}/{table}/{table}_{timestamp}.csv

    Args:
        dataset_id (str): The dataset ID.
        table_name (str): The table ID.

    Returns:
        str: Success message with export path.
    """
    try:
        client = bigquery.Client()
        timestamp = int(time.time())
        destination_uri = f"gs://{BUCKET_NAME}/backup/bq_table/{dataset_id}/{table_name}/{table_name}_{timestamp}.csv"

        dataset_ref = bigquery.DatasetReference(client.project, dataset_id)
        table_ref = dataset_ref.table(table_name)

        extract_job = client.extract_table(
            table_ref,
            destination_uri,
            location="US",
        )
        extract_job.result()  # Wait for export to finish
        return f"✅ Safety Backup exported to: {destination_uri}"
    except Exception as e:
        return f"❌ Critical Backup Failure: {str(e)}"
