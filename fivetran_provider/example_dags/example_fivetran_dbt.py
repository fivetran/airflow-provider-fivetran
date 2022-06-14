from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

from fivetran_provider.operators.fivetran import FivetranOperator
from fivetran_provider.sensors.fivetran import FivetranSensor

from datetime import datetime, timedelta


default_args = {
    "owner": "Airflow",
    "start_date": datetime(2021, 4, 6),
}

with DAG(
    dag_id="ad_reporting_dag",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    linkedin_sync = FivetranOperator(
        task_id="linkedin-ads-sync",
        connector_id="{{ var.value.linkedin_connector_id }}",
    )

    linkedin_sensor = FivetranSensor(
        task_id="linkedin-sensor",
        connector_id="{{ var.value.linkedin_connector_id }}",
        poke_interval=600,
    )

    twitter_sync = FivetranOperator(
        task_id="twitter-ads-sync",
        connector_id="{{ var.value.twitter_connector_id }}",
    )

    twitter_sensor = FivetranSensor(
        task_id="twitter-sensor",
        connector_id="{{ var.value.twitter_connector_id }}",
        poke_interval=600,
    )

    dbt_run = SSHOperator(
        task_id="dbt_ad_reporting",
        command="cd dbt_ad_reporting ; ~/.local/bin/dbt run -m +ad_reporting",
        ssh_conn_id="dbtvm",
    )

    linkedin_sync >> linkedin_sensor
    twitter_sync >> twitter_sensor
    [linkedin_sensor, twitter_sensor] >> dbt_run
