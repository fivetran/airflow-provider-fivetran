import os
import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow_provider_fivetran.operators.fivetran import FivetranOperator
from airflow_provider_fivetran.sensors.fivetran import FivetranSensor


default_args = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(1)
}

dag = DAG(
    dag_id='ad_reporting_dag',
    default_args=default_args
)

linkedin_sync = FivetranOperator(
    task_id='linkedin-ads-sync',
    connector_id=Variable.get("linkedin_connector_id"),
    dag=dag
)


linkedin_sensor = FivetranSensor(
    connector_id=Variable.get("linkedin_connector_id"),
    poke_interval=600,
    task_id='linkedin-sensor',
    dag=dag
)

twitter_sync = FivetranOperator(
    task_id='twitter-ads-sync',
    connector_id=Variable.get("twitter_connector_id"),
    dag=dag
)

twitter_sensor = FivetranSensor(
    connector_id=Variable.get("twitter_connector_id"),
    poke_interval=600,
    task_id='twitter-sensor',
    dag=dag
)

dbt_run = SSHOperator(
    task_id='dbt_ad_reporting',
    command='cd dbt_ad_reporting ; ~/.local/bin/dbt run -m +ad_reporting',
    ssh_conn_id='dbtvm',
    dag=dag
  )

linkedin_sync >> linkedin_sensor
twitter_sync >> twitter_sensor
[linkedin_sensor, twitter_sensor] >> dbt_run
