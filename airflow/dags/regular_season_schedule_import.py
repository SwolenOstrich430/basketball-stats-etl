from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from bball_reference_client.bball_reference_client import BballReferenceClient

dag = DAG(
    dag_id="regular_season_import",
    schedule="* * * 10 10 *",
    start_date=datetime(2000, 1, 1),
    catchup=True,
)


def _download_schedule(**kwargs):
    client = BballReferenceClient()
    logical_date = kwargs['logical_date']
    year = logical_date.year + 1

    raw_schedule = client.get_season_schedule_raw(year)
    print(raw_schedule)


download_regular_season_schedule = PythonOperator(
    task_id="download_regular_season_schedule", 
    python_callable=_download_schedule, 
    dag=dag
)

download_regular_season_schedule >> download_regular_season_schedule >> exit