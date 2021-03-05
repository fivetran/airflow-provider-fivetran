import os
import airflow
from airflow import DAG
from airflow.models import Variable
from operators.fivetran import FivetranOperator



default_args = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(1)
}

dag = DAG(
    dag_id='example_fivetran',
    default_args=default_args
)

fivetran_sync = FivetranOperator(
    task_id='fivetran-task',
    connector_id=Variable.get("connector_id"),
    dag=dag
)

fivetran_sync
