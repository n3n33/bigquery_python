from google.cloud import bigquery

client = bigquery.Client()

datasets = client.list_datasets()

print("Datasets:")
for dataset in datasets:
    print(f"{dataset.project}.{dataset.dataset_id}")
