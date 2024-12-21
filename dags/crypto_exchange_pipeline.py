import json
import requests
from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator


# Constants
GCP_PROJECT = "cryptodatapipeline"
GCS_BUCKET = "hbx-crypto-exchange"
GCS_RAW_DATA_PATH = "raw/crypto_raw"
GCS_TRANSFORMED_PATH = "transformed/crypto_transformed"
BQ_DATASET_NAME = "crypto_exchange"
BQ_TABLE_NAME = "crypto_data"

BQ_SCHEMA = [
    {"name": "id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "symbol", "type": "STRING", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "current_price", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "market_cap", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "total_volume", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "last_updated", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
]


# Fetch data from API
def _fetch_data_from_api():
    url = "https://api.coingecko.com/api/v3/coins/markets"

    headers = {
    "accept": "application/json",
    "x-cg-demo-api-key": "CG-uvD5N3CWPZ6Ps7Aafn8b8qtH\t"
    }

    params = {"vs_currency":"usd",
              "order":"market_cap_desc"}
    
    response = requests.get(url, headers=headers,params=params)

    data = response.json()
    
    with open("crypto_data.json","w") as f:
        json.dump(data,f)
    

# Transform data
def _transform_data():
    with open("crypto_data.json",'r') as f:
        data = json.load(f)
        
    transformed_data = []
    for item in data:
        transformed_data.append({
            'id' : item['id'],
            'symbol' : item['symbol'],
            'name' :item['name'],
            'current_price' : item['current_price'],
            'market_cap' : item['market_cap'],
            'total_valume' : item['total_volume'],
            'last_updated' : item['last_updated'],
            'timestamp' : datetime.utcnow().isoformat()
        })
    df = pd.DataFrame(transformed_data)
    df.to_csv("transformed_data.csv",index=False)




# Define the DAG
default_args = {"owner":"Harish","depends_on_past":False}
dag = DAG(
    dag_id = "CrpytoDataPipline",
    default_args=default_args,
    description= "Fetch data from coingecko api",
    schedule_interval= timedelta(minutes=10),
    start_date= datetime(2024,12,20),
    catchup= False
)

# Fetch data from API
fetch_data_task = PythonOperator(
    task_id="Fetch_data_from_API",
    python_callable=_fetch_data_from_api,
    dag = dag
)

# Create GCS bucket
create_bucket_task = GCSCreateBucketOperator(
    task_id = "create_bucket",
    bucket_name = GCS_BUCKET,
    storage_class = "MULTI_REGIONAL",
    location = "US",
    gcp_conn_id = "google_cloud_default",
    dag = dag
)

# Upload data to GCS
upload_data_to_gcs_task = LocalFilesystemToGCSOperator(
    task_id = "Upload_to_GCS",
    src="crypto_data.json",
    dst = GCS_RAW_DATA_PATH + "_{{ ts_nodash }}.json",
    bucket = GCS_BUCKET,
    gcp_conn_id = "google_cloud_default",
    dag = dag
)

# Transform data
transformed_data_task = PythonOperator(
    task_id = "transformed_data",
    python_callable= _transform_data,
    dag = dag
)

# Upload transformed data to GCS
upload_transformed_data_to_gcs_task = LocalFilesystemToGCSOperator(
    task_id = "Upload_transformed_to_GCS",
    src="transformed_data.csv",
    dst = GCS_TRANSFORMED_PATH + "_{{ ts_nodash }}.csv",
    bucket = GCS_BUCKET,
    gcp_conn_id = "google_cloud_default",
    dag = dag
)

# Create BigQuery dataset
create_bq_dataset_task = BigQueryCreateEmptyDatasetOperator(
    task_id = "create_bq_dataset",
    dataset_id = BQ_DATASET_NAME,
    project_id = GCP_PROJECT,
    location = "US",
    gcp_conn_id = "google_cloud_default",
    dag = dag
)

# Create BigQuery table
create_bq_table_task = BigQueryCreateEmptyTableOperator(
    task_id = "create_bq_table",
    dataset_id = BQ_DATASET_NAME,
    table_id = BQ_TABLE_NAME,
    project_id = GCP_PROJECT,
    schema_fields = BQ_SCHEMA,
    gcp_conn_id = "google_cloud_default",
    dag = dag
)

# GCS to BigQuery
load_to_bq_task = GCSToBigQueryOperator(
    task_id = "load_to_bq",
    bucket = GCS_BUCKET,
    source_objects = [GCS_TRANSFORMED_PATH + "_{{ ts_nodash }}.csv"],
    destination_project_dataset_table = f"{GCP_PROJECT}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}",
    schema_fields = BQ_SCHEMA,
    write_disposition = "WRITE_APPEND",
    skip_leading_rows = 1,
    field_delimiter = ",",
    gcp_conn_id = "google_cloud_default",
    dag = dag
)


fetch_data_task >> create_bucket_task >> upload_data_to_gcs_task
upload_data_to_gcs_task >> transformed_data_task >> upload_transformed_data_to_gcs_task
upload_transformed_data_to_gcs_task >> create_bq_dataset_task >> create_bq_table_task >> load_to_bq_task





