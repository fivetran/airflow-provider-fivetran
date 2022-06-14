from airflow import DAG

from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python_operator import BranchPythonOperator
from fivetran_provider.operators.fivetran import FivetranOperator
from fivetran_provider.sensors.fivetran import FivetranSensor
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
    BigQueryGetDataOperator,
)

from datetime import datetime, timedelta

# EDIT WITH YOUR PROJECT ID & DATASET NAME
PROJECT_ID = "YOUR PROJECT ID"
DATASET_NAME = "bqml"
DESTINATION_TABLE = "dbt_ads_bqml_preds"

TRAINING_QUERY = (
    "CREATE OR REPLACE MODEL bqml.dbt_ads_airflow_model "
    "OPTIONS "
    "(model_type = 'ARIMA_PLUS', "
    "time_series_timestamp_col = 'parsed_date', "
    "time_series_data_col = 'daily_impressions', "
    "auto_arima = TRUE, "
    "data_frequency = 'AUTO_FREQUENCY', "
    "decompose_time_series = TRUE "
    ") AS "
    "SELECT "
    "timestamp(date_day) as parsed_date, "
    "SUM(impressions) as daily_impressions "
    "FROM  `" + PROJECT_ID + ".bqml.ad_reporting` "
    "GROUP BY date_day;"
)

SERVING_QUERY = (
    "SELECT string(forecast_timestamp) as forecast_timestamp, "
    "forecast_value, "
    "standard_error, "
    "confidence_level, "
    "prediction_interval_lower_bound, "
    "prediction_interval_upper_bound, "
    "confidence_interval_lower_bound, "
    "confidence_interval_upper_bound "
    "FROM ML.FORECAST(MODEL `"
    + PROJECT_ID
    + ".bqml.dbt_ads_airflow_model`,STRUCT(30 AS horizon, 0.8 AS confidence_level));"
)


def ml_branch(ds, **kwargs):
    if "train" in kwargs["params"] and kwargs["params"]["train"]:
        return "train_model"
    else:
        return "get_predictions"


default_args = {
    "owner": "Airflow",
    "start_date": datetime(2021, 4, 6),
}

dag = DAG(
    dag_id="example_fivetran_bqml",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
)

with dag:
    linkedin_sync = FivetranOperator(
        task_id="linkedin-sync",
        fivetran_conn_id="fivetran_default",
        connector_id="{{ var.value.linkedin_connector_id }}",
    )

    linkedin_sensor = FivetranSensor(
        task_id="linkedin-sensor",
        fivetran_conn_id="fivetran_default",
        connector_id="{{ var.value.linkedin_connector_id }}",
        poke_interval=5,
    )

    twitter_sync = FivetranOperator(
        task_id="twitter-sync",
        fivetran_conn_id="fivetran_default",
        connector_id="{{ var.value.twitter_connector_id }}",
    )

    twitter_sensor = FivetranSensor(
        task_id="twitter-sensor",
        fivetran_conn_id="fivetran_default",
        connector_id="{{ var.value.twitter_connector_id }}",
        poke_interval=5,
    )

    dbt_run = SSHOperator(
        task_id="dbt_ad_reporting",
        command="cd dbt_ad_reporting ; ~/.local/bin/dbt run -m +ad_reporting",
        ssh_conn_id="dbtvm",
    )

    ml_branch = BranchPythonOperator(
        task_id="ml_branch", python_callable=ml_branch, provide_context=True
    )

    train_model = BigQueryExecuteQueryOperator(
        task_id="train_model", sql=TRAINING_QUERY, use_legacy_sql=False
    )

    get_preds = BigQueryExecuteQueryOperator(
        task_id="get_predictions",
        sql=SERVING_QUERY,
        use_legacy_sql=False,
        destination_dataset_table=DATASET_NAME + "." + DESTINATION_TABLE,
        write_disposition="WRITE_APPEND",
    )

    print_preds = BigQueryGetDataOperator(
        task_id="print_predictions", dataset_id=DATASET_NAME, table_id=DESTINATION_TABLE
    )

    linkedin_sync >> linkedin_sensor
    twitter_sync >> twitter_sensor

    [linkedin_sensor, twitter_sensor] >> dbt_run

    dbt_run >> ml_branch >> [train_model, get_preds]
    get_preds >> print_preds
