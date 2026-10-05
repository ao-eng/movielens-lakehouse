import os
import io
import zipfile
import requests
import duckdb
from datetime import datetime
from airflow.decorators import dag, task

@dag(
    dag_id="ingestion_movielens_duckdb",
    schedule="@once",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ingestao", "duckdb", "s3"],
)
def ingestion_duck_pipeline():

    @task
    def extract_movielens_duck_s3():
        # collect container python env vars
        aws_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
        aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-2")

        if not aws_key or not aws_secret:
            raise ValueError("Missing AWS credentials... (.env)!")
        
        # memory duckdb connection
        con = duckdb.connect(database=":memory:")
        
        # load required duckdb modules
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("INSTALL zipfs FROM community;LOAD zipfs;")
        
        # set aws credentials
        con.execute(f"""
            SET s3_region='{aws_region}';
            SET s3_access_key_id='{aws_key}';
            SET s3_secret_access_key='{aws_secret}';
        """)
        
        # movielens url
        movielens_zip_url = "https://files.grouplens.org/datasets/movielens/ml-20m.zip"

        #This query extracts the list of valid files from the zip file
        #this approach is better than exporting zip content into content variables and then flushing it to the target
        list_of_files = con.execute(f"""
            SELECT list(parse_filename(file_name,true)) as file_name
            FROM zip_contents('{movielens_zip_url}')
            where ends_with(file_name, '.csv') = True
        """).fetchone()[0]

        print(list_of_files)

        for file in list_of_files:
        
            # Duckdb read csv data inside zip file and write parquet data on S3
            con.execute(f"""
                COPY (
                    select * from 'zip://{movielens_zip_url}/*/{file}.csv'
                ) TO 's3://ao-eng/movielens/raw/{file}/{file}.parquet' (FORMAT PARQUET,OVERWRITE TRUE);
            """)
        
        con.close()
        print("Ingestão do MovieLens para S3 concluída com sucesso!")

    extract_movielens_duck_s3()

ingestion_duck_pipeline()