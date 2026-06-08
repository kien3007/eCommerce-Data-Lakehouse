from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_SUBMIT_BASE = (
    'docker exec spark-master /opt/spark/bin/spark-submit '
    '--master spark://spark-master:7077 '
    '--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,'
    'org.apache.iceberg:iceberg-aws-bundle:1.5.0,'
    'org.apache.hadoop:hadoop-aws:3.3.4 '
    '--driver-memory 2g --executor-memory 2g '
    '--py-files /opt/spark_jobs/spark_utils.py '
)

with DAG(
    'dag_daily_medallion_etl',
    default_args={'owner': 'airflow', 'start_date': datetime(2026, 6, 1)},
    schedule='@daily',
    catchup=False,
    tags=['spark', 'iceberg', 'medallion', 'daily'],
    doc_md="""
    ## Daily Medallion Pipeline
    Runs automatically every midnight to process new records.
    1. **Bronze → Silver** — Clean new logs and join with Cohort mapping.
    2. **Silver → Gold** — Recalculate Aggregated Analytics (RFM, Funnel).
    """
) as dag:

    run_bronze_to_silver = BashOperator(
        task_id='run_bronze_to_silver',
        bash_command=SPARK_SUBMIT_BASE + '/opt/spark_jobs/bronze_to_silver.py'
    )

    run_silver_to_gold = BashOperator(
        task_id='run_silver_to_gold',
        bash_command=SPARK_SUBMIT_BASE + '/opt/spark_jobs/silver_to_gold.py'
    )

    run_bronze_to_silver >> run_silver_to_gold
