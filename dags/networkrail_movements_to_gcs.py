import csv
import json
import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator 
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone
from airflow.models import Variable

from google.cloud import bigquery, storage
from google.oauth2 import service_account


DAGS_FOLDER = "/opt/airflow/dags"
BUSINESS_DOMAIN = "networkrail"
DATA = "movements"
LOCATION = "asia-southeast1"
PROJECT_ID = "dataengineerbootcamp"
GCS_BUCKET = "railway-bucket"
BIGQUERY_DATASET = "railway_dataset"
VARIABLE_GCS_CREDENTIAL = "gcs_credential_secret"

def _extract_data(**context):
    ds = context["data_interval_start"].to_date_string()

    # If we have data, go to the "load_data_to_gcs" task; otherwise, 
    # go to the "do_nothing" task
    pg_hook = PostgresHook(
        postgres_conn_id="networkrail_postgres_conn",
        schema="networkrail"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = f"""
        select * from movements where date(actual_timestamp) = '{ds}'
    """
    logging.info(sql)

    cursor.execute(sql)
    rows = cursor.fetchall()

    if rows:
        with open(f"{DAGS_FOLDER}/{DATA}-{ds}.csv", "w") as f:
            writer = csv.writer(f)
            header = [
                "event_type",
                "gbtt_timestamp",
                "original_loc_stanox",
                "planned_timestamp",
                "timetable_variation",
                "original_loc_timestamp",
                "current_train_id",
                "delay_monitoring_point",
                "next_report_run_time",
                "reporting_stanox",
                "actual_timestamp",
                "correction_ind",
                "event_source",
                "train_file_address",
                "platform",
                "division_code",
                "train_terminated",
                "train_id",
                "offroute_ind",
                "variation_status",
                "train_service_code",
                "toc_id",
                "loc_stanox",
                "auto_expected",
                "direction_ind",
                "route",
                "planned_event_type",
                "next_report_stanox",
                "line_ind",
            ]
            writer.writerow(header)
            for row in rows:
                logging.info(row)
                writer.writerow(row)

        return "load_data_to_gcs"
    else:
        return "do_nothing"


def _load_data_to_gcs(**context):
    ds = context["data_interval_start"].to_date_string()
    gcs_secret = Variable.get(VARIABLE_GCS_CREDENTIAL, deserialize_json=True)
    credentials = service_account.Credentials.from_service_account_info(gcs_secret)
    storage_client = storage.Client(
        project=PROJECT_ID,
        credentials=credentials,
    )
    bucket = storage_client.bucket(GCS_BUCKET)
    destination_blob_name = f"{BUSINESS_DOMAIN}/{DATA}/{ds}/{DATA}.csv"
    source_file_name = f"{DAGS_FOLDER}/{DATA}-{ds}.csv"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )
    # Your code here



default_args = {
    "owner": "Patsawut Tananchai",
    "start_date": timezone.datetime(2023, 5, 1),
}
with DAG(
    dag_id="networkrail_movements_to_gcs",
    default_args=default_args,
    schedule="@daily",  # Set the schedule here 
    catchup=False,
    tags=["DEB", "2023", "networkrail"],
    max_active_runs=1,
):

    # Start
    start = EmptyOperator(task_id="start")

    # Extract data from NetworkRail Postgres Database
    extract_data = BranchPythonOperator(
        task_id="extract_data"
        ,python_callable=_extract_data)

    # Do nothing
    do_nothing = EmptyOperator(task_id="do_nothing")

    # Load data to GCS
    load_data_to_gcs = PythonOperator(
        task_id="load_data_to_gcs"
        ,python_callable=_load_data_to_gcs)

    # trigger another dag
    trigger_networkrail_movements_gcs_to_bcq = TriggerDagRunOperator(
        task_id='trigger_networkrail_movements_gcs_to_bcq'
        ,trigger_dag_id='networkrail_movements_gcs_to_bcq'
        ,execution_date='{{ ds }}'
        )
    
    #end
    end = EmptyOperator(task_id="end", trigger_rule="one_success")

    # Task dependencies
    start >> extract_data >> load_data_to_gcs  >> trigger_networkrail_movements_gcs_to_bcq >> end
    extract_data >> do_nothing >> end