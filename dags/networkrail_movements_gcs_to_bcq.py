import csv
import json
import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
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
VARIABLE_BCQ_CREDENTIAL = "bcq_credential_secret"




def _load_data_from_gcs_to_bigquery(**context):

    bcq_secret = Variable.get(VARIABLE_BCQ_CREDENTIAL, deserialize_json=True)
    credentials_bcq = service_account.Credentials.from_service_account_info(bcq_secret)
    bigquery_client = bigquery.Client(
        project=PROJECT_ID,
        credentials=credentials_bcq,
        location=LOCATION,
    )
    ds = context["data_interval_start"].to_date_string()

    bigquery_schema = [
        bigquery.SchemaField("event_type", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("gbtt_timestamp", bigquery.enums.SqlTypeNames.TIMESTAMP),
        bigquery.SchemaField("original_loc_stanox", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("planned_timestamp", bigquery.enums.SqlTypeNames.TIMESTAMP),
        bigquery.SchemaField("timetable_variation", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("original_loc_timestamp", bigquery.enums.SqlTypeNames.TIMESTAMP),
        bigquery.SchemaField("current_train_id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("delay_monitoring_point", bigquery.enums.SqlTypeNames.BOOLEAN),
        bigquery.SchemaField("next_report_run_time", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("reporting_stanox", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("actual_timestamp", bigquery.enums.SqlTypeNames.TIMESTAMP),
        bigquery.SchemaField("correction_ind", bigquery.enums.SqlTypeNames.BOOLEAN),
        bigquery.SchemaField("event_source", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("train_file_address", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("platform", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("division_code", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("train_terminated", bigquery.enums.SqlTypeNames.BOOLEAN),
        bigquery.SchemaField("train_id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("offroute_ind", bigquery.enums.SqlTypeNames.BOOLEAN),
        bigquery.SchemaField("variation_status", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("train_service_code", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("toc_id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("loc_stanox", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("auto_expected", bigquery.enums.SqlTypeNames.BOOLEAN),
        bigquery.SchemaField("direction_ind", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("route", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("planned_event_type", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("next_report_stanox", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("line_ind", bigquery.enums.SqlTypeNames.STRING),
    ]
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        schema=bigquery_schema,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="actual_timestamp",
        ),
    )
    partition = ds.replace("-", "")
    table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{DATA}${partition}"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{DATA}/{ds}/*.csv"
    job = bigquery_client.load_table_from_uri(
        f"gs://{GCS_BUCKET}/{destination_blob_name}",
        table_id,
        job_config=job_config,
        location=LOCATION,
    )
    job.result()
    table = bigquery_client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")



default_args = {
    "owner": "Patsawut Tananchai",
    "start_date": timezone.datetime(2023, 5, 1),
}
with DAG(
    dag_id="networkrail_movements_gcs_to_bcq",
    default_args=default_args,
    schedule=None,  # Set the schedule here
    catchup=False,
    tags=["DEB", "2023", "networkrail"],
    max_active_runs=3,
):

    # Start
    start = EmptyOperator(task_id="start")


    # Load data from GCS to BigQuery
    load_data_from_gcs_to_bigquery = PythonOperator(
        task_id="load_data_from_gcs_to_bigquery"
        ,python_callable=_load_data_from_gcs_to_bigquery)

    # End
    end = EmptyOperator(task_id="end", trigger_rule="one_success")

    # Task dependencies
    start  >> load_data_from_gcs_to_bigquery  >> end