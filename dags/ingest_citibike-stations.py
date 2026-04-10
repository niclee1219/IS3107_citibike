from google.cloud.exceptions import NotFound
from google.cloud import bigquery
import pandas as pd

GC_PROJECT_ID = 'is3107-491906'

def load_static_station_to_bq(csv_file_path, project_id=GC_PROJECT_ID, dataset_id='citibike', table_id='static_stations'):
    '''
    Loads static station data from a CSV file into BigQuery.
    Data will be handled using UPSERT logic: 
        - if a station with the same short_name exists, it will be updated
        - else, it will be inserted as a new record.
    '''
    # BQ setup
    client = bigquery.Client(project=project_id)
    staging_table_id = f'{project_id}.{dataset_id}.staging_{table_id}'
    prod_table_id = f'{project_id}.{dataset_id}.{table_id}'

    # ingest CSV file && remove BOM
    df = pd.read_csv(csv_file_path, encoding='utf-8-sig')

    ###### error handlling: create datasets/tables if not exists #########
    # check datasets
    dataset_ref = f"{project_id}.{dataset_id}"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "asia-east1" # taiwan
    client.create_dataset(dataset, exists_ok=True)
    # https://docs.cloud.google.com/bigquery/docs/tables#python
    try:
        client.get_table(staging_table_id, retry=None)
        client.get_table(prod_table_id, retry=None)
    except NotFound:
        schema = [
            bigquery.SchemaField("short_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("lat", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("lon", "FLOAT", mode="REQUIRED"),
        ]
        table = bigquery.Table(staging_table_id, schema=schema)
        table = client.create_table(table, exists_ok=True)

        table_prod = bigquery.Table(prod_table_id, schema=schema)
        table_prod = client.create_table(table_prod, exists_ok=True)
    ######## end error handling #########

    # load into staging table
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # overwrite staging table
    )
    load_job = client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
    load_job.result()  # Waits for the job to complete.

    # merge staging table into prod table (upsert)
    MERGE_QUERY = f"""
    MERGE `{prod_table_id}` TARGET
    USING `{staging_table_id}` SOURCE
    ON TARGET.short_name = SOURCE.short_name
    WHEN MATCHED THEN
        UPDATE SET name = SOURCE.name, lat = SOURCE.lat, lon = SOURCE.lon
    WHEN NOT MATCHED THEN
        INSERT (short_name, name, lat, lon) VALUES (SOURCE.short_name, SOURCE.name, SOURCE.lat, SOURCE.lon)
    """
    merge_job = client.query(MERGE_QUERY)
    merge_job.result()  # Waits for the job to complete.

    # cleanup staging table, truncate (use truncate instead of delete to avoid incurring delete costs)
    # client.query(f'TRUNCATE TABLE `{staging_table_id}`').result()
    
if __name__ == "__main__":
    load_static_station_to_bq('./output/citibike_stations/stations.csv')

