import time
from airflow import DAG

from fivetran_provider.operators.fivetran import FivetranOperator
from fivetran_provider.sensors.fivetran import FivetranSensor
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta


default_args = {
    "owner": "Airflow",
    "start_date": datetime(2021, 4, 6),
    "provide_context": True,
}

dag = DAG(
    dag_id="example_fivetran_xcom",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
)

with dag:
    fivetran_operator = FivetranOperator(
        task_id="fivetran-operator",
        fivetran_conn_id="fivetran_default",
        connector_id="{{ var.value.connector_id }}",
    )

    delay_task = PythonOperator(
        task_id="delay_python_task", python_callable=lambda: time.sleep(60)
    )

    fivetran_sensor = FivetranSensor(
        task_id="fivetran-sensor",
        fivetran_conn_id="fivetran_default",
        connector_id="{{ var.value.connector_id }}",
        poke_interval=5,
        xcom="{{ task_instance.xcom_pull('fivetran-operator', key='return_value') }}",
    )

    fivetran_operator >> delay_task >> fivetran_sensor
