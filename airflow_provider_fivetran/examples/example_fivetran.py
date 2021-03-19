import os
import airflow
from airflow import DAG
from airflow.models import Variable
from airflow_provider_fivetran.operator.fivetran import FivetranOperator
from airflow_provider_fivetran.sensor.fivetran import FivetranSensor


default_args = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(1)
}

dag = DAG(
    dag_id='example_fivetran',
    default_args=default_args
)

start_fivetran_sync = FivetranOperator(
    task_id='fivetran-task',
    connector_id=Variable.get("connector_id"),
    dag=dag
)

wait_fivetran_sync = FivetranSensor(
    connector_id=Variable.get("connector_id"),
    poke_interval=5,
    task_id='fivetran-sensor',
    dag=dag
)

start_fivetran_sync >> wait_fivetran_sync
