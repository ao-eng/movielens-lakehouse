import os
import io
import zipfile
import requests
import polars as pl
from datetime import datetime
from airflow.sdk import Asset, dag, task

MOVIELENS_RAW = Asset("movielens_raw")

@dag(
    dag_id="ingestion_movielens_polars",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ingestion", "polars", "s3", "dynamic"],
)

def ingestion_polars_pipeline():

    @task(outlets=[MOVIELENS_RAW])
    def extract_zip_to_s3():
        aws_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
        aws_region = os.getenv("AWS_DEFAULT_REGION","us-east-1")
        bucket = "ao-eng"

        # Opções de armazenamento S3 para o Polars/PyArrow
        storage_options = {
            "aws_access_key_id": aws_key,
            "aws_secret_access_key": aws_secret,
            "aws_region": aws_region,
        }

        movielens_zip_url = "https://files.grouplens.org/datasets/movielens/ml-20m.zip"
        response = requests.get(movielens_zip_url)
        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_files = [f for f in z.namelist() if f.endswith('.csv')]
            for csv_file in csv_files:
                table_name = os.path.basename(csv_file).replace(".csv","")
                print(f"Reading '{table_name}' with Polars...")

                with z.open(csv_file) as f:
                    df = pl.read_csv(f.read())

                s3_target_path = f"s3://{bucket}/movielens/raw/{table_name}/{table_name}.parquet"
                df.write_parquet(
                    s3_target_path,
                    storage_options=storage_options,
                    compression='snappy'
                )
                print(f"Table '{table_name}' written successfuly via Polars on: {s3_target_path}")

    extract_zip_to_s3()
ingestion_polars_pipeline()

