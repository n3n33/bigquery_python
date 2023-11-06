from datetime import datetime
from airflow import models
from airflow.providers.google.cloud.operators import dataproc, bigquery
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteTableOperator,
)
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
)

SUBNETWORK_URI = models.variable.Variable.get("subnetwork_uri")
TAGS = models.variable.Variable.get("tags")

default_config = {
    "HDFS_PATH" : "hdfs://?/apps/hive/warehouse?",
    "GS_PATH" : "gs://?/?",
    "DATASET_NAME" : "?",
    "BUCKET_NAME" : "?",
    "GCS_OBJECT" : "/?/?/*",
    "TABLE_NAME" : "?",
    "DST_TABLE_NAME" : "?"
}

default_dag_args = {
    "start_date": datetime(2023,10,12),
    "owner": "naya",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "project_id": "?",
    "region": "asia-northeast3",
}

CLUSTER_NAME = "distcp-datadev-cluster"
CLUSTER_CONFIG = {
    "gce_cluster_config": {
        "subnetwork_uri": SUBNETWORK_URI,
        "tags": [TAGS]
    },
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "e2-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32}
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "e2-standard-8",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32}
    },
}

HADOOP_JOB = {
        "reference": {"project_id": "?"},
        "placement": {"cluster_name": CLUSTER_NAME},
        "hadoop_job": {
            "main_jar_file_uri": "file:///usr/lib/hadoop/hadoop-distcp.jar",
            "main_class": "org.apache.hadoop.tools.DistCp",
            "args": [
                "-m", "20", "-delete", "-overwrite", default_config["HDFS_PATH"], default_config["GS_PATH"]
            ]
        },
    }

new_column_names = ["test"]

def parquet_insert_table():
    client = bigquery.Client(project=default_dag_args['project_id'])
    dataset_ref = client.dataset(default_config['DATASET_NAME'])
    table_ref = dataset_ref.table(default_config['TABLE_NAME'])
    parquet_uri = f"{default_config['GS_PATH']}/*"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    ) 

    load_job = client.load_table_from_uri(
        parquet_uri,
        table_ref,
        job_config=job_config
    )
    load_job.result()

   
def create_dst_table():
    client = bigquery.Client(project=default_dag_args['project_id'])
    dataset_ref = client.dataset(default_config['DATASET_NAME'])
    table_ref = dataset_ref.table(default_config['TABLE_NAME'])
    table = client.get_table(table_ref)
    updated_schema = []
    for idx, field in enumerate(table.schema):
        updated_schema.append(bigquery.SchemaField(new_column_names[idx], field.field_type, mode=field.mode))

    new_table_ref = dataset_ref.table(default_config['DST_TABLE_NAME'])
    new_table = bigquery.Table(new_table_ref, schema=updated_schema)
    client.create_table(new_table)
    
    query = f"INSERT INTO `{default_dag_args['project_id']}.{default_config['DATASET_NAME']}.{default_config['DST_TABLE_NAME']}` SELECT * FROM `{default_dag_args['project_id']}.{default_config['DATASET_NAME']}.{default_config['TABLE_NAME']}`"
    query_job = client.query(query)
    query_job.result() 
    
    
with models.DAG(
    "test_dag",
    default_args=default_dag_args,
    schedule_interval='15 9 * * *',
    tags = ["test"]
) as dag:
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=default_dag_args['project_id'],
        cluster_config=CLUSTER_CONFIG,
        region=default_dag_args['region'],
        cluster_name=CLUSTER_NAME,
    )
    
    run_dataproc_hadoop = DataprocSubmitJobOperator(
        task_id="run_dataproc_hadoop", job=HADOOP_JOB
    )
    
    delete_dst_table = BigQueryDeleteTableOperator(
        task_id="delete_table",
        deletion_dataset_table=f"{default_dag_args['project_id']}.{default_config['DATASET_NAME']}.{default_config['DST_TABLE_NAME']}",
    )   
    
    insert_parquet_data = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=parquet_insert_table,
    )
    
    create_dst_data = PythonOperator(
        task_id='create_bigquery_table',
        python_callable=create_dst_table,
    )
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=default_dag_args['project_id'],
        cluster_name=CLUSTER_NAME,
        region=default_dag_args['region'],
    )
    
    delete_stage_table = BigQueryDeleteTableOperator(
        task_id="delete_stage_table",
        deletion_dataset_table=f"{default_dag_args['project_id']}.{default_config['DATASET_NAME']}.{default_config['TABLE_NAME']}",
    )  
    create_cluster >> run_dataproc_hadoop >> insert_parquet_data >> delete_dst_table >> create_dst_data >> delete_stage_table >> delete_cluster
