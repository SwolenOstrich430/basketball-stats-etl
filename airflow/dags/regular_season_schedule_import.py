import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import importlib

def get_bball_client():
    module = importlib.import_module(
        "bball_reference_client.bball_reference_client"
    )
    return module.BballReferenceClient()

@task
def download_schedule(logical_date=None):
    client = get_bball_client()
    year = logical_date.year + 1
    raw_schedule = client.get_schedule_raw(year)

    # Correct filename
    filename = f"/tmp/{logical_date.year}_{year}_regular_season_schedule.json"
    raw_schedule.to_json(filename)

    return filename 


@task
def upload_to_gcs(src):
    LocalFilesystemToGCSOperator(
        task_id="upload",
        src=src,
        dst=f"etl/upload/{src.split('/')[-1]}",
        bucket="basketball-stats",
        gzip=True
    ).execute({})   # must manually execute inside @task


with DAG(
    dag_id="regular_season_import",
    schedule="0 0 10 10 *",
    start_date=pendulum.datetime(2000, 1, 1),
    catchup=True,
) as dag:

    file_path = download_schedule()
    upload_to_gcs(file_path)