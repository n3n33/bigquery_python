#!/usr/bin/env python3
from google.cloud import bigquery
from pandas import read_parquet

# BigQuery Client Create
client = bigquery.Client()

# BigQuery Teable Info
project_id = 'your_project_name'
dataset_id = 'your_dataset_name'
table_id = 'table_name'
table_ref = f"{project_id}.{dataset_id}.{table_id}"

# Read Parquet 
parquet_file = '/your/parquet/file/path'
df = read_parquet(parquet_file)

# Create Table and Data Loas
job_config = bigquery.LoadJobConfig()
job_config.autodetect = True

job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
job.result()  
