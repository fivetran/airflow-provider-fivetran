from airflow import DAG, AirflowException
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperator
from fivetran_provider.operators.fivetran import FivetranOperator
from fivetran_provider.sensors.fivetran import FivetranSensor
from airflow.utils.dates import datetime


TABLE = "forestfires"
DATASET = "google_sheets"
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "astronomer",
    "depends_on_past": False,
    "start_date": datetime(2021, 7, 7),
    "email": ["noreply@astronomer.io"],
    "email_on_failure": False,
}

with DAG(
    "example_fivetran_bigquery",
    default_args=default_args,
    description="",
    schedule_interval=None,
    catchup=False,
) as dag:
    """
    ### Simple EL Pipeline with Data Integrity and Quality Checks
    Before running the DAG, set the following in an Airflow or Environment Variables:
    - key: gcp_project_id
      value: [gcp_project_id]
    - key: connector_id
      value: [connector_id]
    Fully replacing [gcp_project_id] & [connector_id] with the actual IDs.
    What makes this a simple data quality case is:
    1. Absolute ground truth: the local CSV file is considered perfect and immutable.
    2. No transformations or business logic.
    3. Exact values of data to quality check are known.
    """

    """
    #### FivetranOperator & FivetranSensor
    Calling Fivetran to begin data movement from Google Sheets to BigQuery
    The FivetranSensor monitors the status of the Fivetran data sync
    """
    fivetran_sync_start = FivetranOperator(
        task_id="fivetran-task",
        fivetran_conn_id="fivetran_default",
        connector_id="{{ var.value.connector_id }}",
    )

    fivetran_sync_wait = FivetranSensor(
        task_id="fivetran-sensor",
        fivetran_conn_id="fivetran_default",
        connector_id="{{ var.value.connector_id }}",
        poke_interval=5,
    )

    """
    #### BigQuery row validation task
    Ensure that data was copied to BigQuery correctly, i.e. the table and dataset
    exist.
    """
    validate_bigquery = BigQueryTableExistenceSensor(
        task_id="validate_bigquery",
        project_id="{{ var.value.gcp_project_id }}",
        dataset_id=DATASET,
        table_id="forestfires",
    )

    """
    #### Row-level data quality check
    Run a data quality check on a few rows, ensuring that the data in BigQuery
    matches the ground truth in the correspoding JSON file.
    """
    check_bq_row_count = BigQueryValueCheckOperator(
        task_id="check_row_count",
        sql=f"SELECT COUNT(*) FROM {DATASET}.{TABLE}",
        pass_value=516,
        use_legacy_sql=False,
    )

    done = DummyOperator(task_id="done")

    fivetran_sync_start >> fivetran_sync_wait >> validate_bigquery
    validate_bigquery >> check_bq_row_count >> done
