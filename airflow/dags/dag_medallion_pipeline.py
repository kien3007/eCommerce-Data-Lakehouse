import os
import logging
from datetime import datetime
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def upload_to_minio():
    """
    Uploads the static purchase behavior CSV file to the raw layer in MinIO.
    This simulates a batch ingestion process of historical static data.
    """
    endpoint = os.getenv('MINIO_ENDPOINT', 'http://minio:9000') # Endpoint safe to default
    access_key = os.environ['MINIO_ROOT_USER']
    secret_key = os.environ['MINIO_ROOT_PASSWORD']

    s3_client = boto3.client('s3',
                             endpoint_url=endpoint,
                             aws_access_key_id=access_key,
                             aws_secret_access_key=secret_key)

    bucket_name = 'raw'
    file_path = '/opt/dataset/02-purchase-behavior.csv'
    object_name = 'purchase-behavior/02-purchase-behavior.csv'

    logger.info(f"Starting upload of {file_path} to s3://{bucket_name}/{object_name}")
    s3_client.upload_file(file_path, bucket_name, object_name)
    logger.info("Upload to MinIO completed successfully.")


SPARK_SUBMIT_BASE = (
    'docker exec spark-master /opt/spark/bin/spark-submit '
    '--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,'
    'org.apache.iceberg:iceberg-aws-bundle:1.5.0,'
    'org.apache.hadoop:hadoop-aws:3.3.4 '
    '--py-files /opt/spark_jobs/spark_utils.py '
)

with DAG(
    'medallion_pipeline',
    default_args={'owner': 'airflow', 'start_date': datetime(2026, 6, 1)},
    schedule_interval='@daily',
    catchup=False,
    tags=['spark', 'iceberg', 'medallion'],
    doc_md="""
    ## Medallion Architecture Pipeline
    End-to-end batch ETL pipeline:
    1. **Ingest** — Upload static CSV to MinIO raw layer
    2. **Bronze → Silver** — Enrich logs with cohort data, deduplicate
    3. **Silver → Gold** — Generate 9 analytics tables (RFM, Cohort, Funnel, etc.)
    """
) as dag:

    # Task 1: Upload static dataset to MinIO
    upload_csv = PythonOperator(
        task_id='upload_csv_to_minio',
        python_callable=upload_to_minio
    )

    # Task 2: Run Bronze to Silver Spark job
    run_bronze_to_silver = BashOperator(
        task_id='run_bronze_to_silver',
        bash_command=SPARK_SUBMIT_BASE + '/opt/spark_jobs/bronze_to_silver.py'
    )

    # Task 3: Run Silver to Gold Spark job
    run_silver_to_gold = BashOperator(
        task_id='run_silver_to_gold',
        bash_command=SPARK_SUBMIT_BASE + '/opt/spark_jobs/silver_to_gold.py'
    )

    # Dependency chain: upload first, then Bronze→Silver, then Silver→Gold
    upload_csv >> run_bronze_to_silver >> run_silver_to_gold
