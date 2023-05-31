import csv
import json
import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
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
POSTGRES_CONNECTION_ID = "networkrail_postgres_conn"
GCS_CONN_ID = "gcs_connection"


default_args = {
    "owner": "Patsawut Tananchai",
    "start_date": timezone.datetime(2023, 5, 1),
}

with DAG(
    dag_id="networkrail_movements_to_gcs_gcs_operator",
    default_args=default_args,
    schedule="@daily",  # Set the schedule here
    catchup=False,
    tags=["DEB", "2023", "networkrail"],
    max_active_runs=1,
):

    # Start "networkrail/movements/{{ds}}/movements.csv"
    start = EmptyOperator(task_id="start")

    postgres_to_gcs = PostgresToGCSOperator(
        task_id="postgres_to_gcs",
        postgres_conn_id=POSTGRES_CONNECTION_ID,
        sql= "SELECT * FROM movements WHERE TO_CHAR( DATE(actual_timestamp) , 'YYYY-MM-DD' ) =  '{{ ds }}' ",
        bucket=GCS_BUCKET,
        filename="networkrail/movements/{{ds}}/movements.csv",
        export_format='csv',
        gzip=False,
        use_server_side_cursor=True,
        gcp_conn_id=GCS_CONN_ID,
    )

    # End
    end = EmptyOperator(task_id="end", trigger_rule="one_success")

    # Task dependencies
    start >> postgres_to_gcs  >> end

