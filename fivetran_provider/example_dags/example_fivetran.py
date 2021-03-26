import airflow
from airflow import DAG
from airflow.models import Variable
from fivetran_provider.operators.fivetran import FivetranOperator
from fivetran_provider.sensors.fivetran import FivetranSensor


default_args = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(1)
}

dag = DAG(
    dag_id='example_fivetran',
    default_args=default_args
)

fivetran_sync_start = FivetranOperator(
    task_id='fivetran-task',
    fivetran_conn_id='fivetran_default',
    connector_id="{{ var.value.get('connector_id') }}",
    dag=dag
)

fivetran_sync_wait = FivetranSensor(
    task_id='fivetran-sensor',
    fivetran_conn_id='fivetran_default',
    connector_id="{{ var.value.get('connector_id') }}",
    poke_interval=5,
    dag=dag
)

fivetran_sync_start >> fivetran_sync_wait
