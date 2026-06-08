from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_SUBMIT_BASE = (
    'docker exec spark-master /opt/spark/bin/spark-submit '
    '--master spark://spark-master:7077 '
    '--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,'
    'org.apache.iceberg:iceberg-aws-bundle:1.5.0,'
    'org.apache.hadoop:hadoop-aws:3.3.4 '
    '--py-files /opt/spark_jobs/spark_utils.py '
)

with DAG(
    'dag_iceberg_maintenance',
    default_args={'owner': 'airflow', 'start_date': datetime(2026, 6, 1)},
    schedule='0 2 * * *', # Run at 2 AM every day
    catchup=False,
    tags=['spark', 'iceberg', 'maintenance'],
    doc_md="""
    ## Iceberg Maintenance Pipeline
    Runs automatically at 2:00 AM daily to optimize Iceberg tables:
    1. **Compaction:** Rewrites small data files into larger ones to improve read performance (solves the Streaming small files issue).
    2. **Expire Snapshots:** Cleans up old snapshots to reclaim storage space.
    3. **Rewrite Manifests:** Optimizes metadata files for faster query planning.
    """
) as dag:

    run_iceberg_maintenance = BashOperator(
        task_id='run_iceberg_maintenance',
        bash_command=SPARK_SUBMIT_BASE + '/opt/spark_jobs/iceberg_maintenance.py'
    )

    run_iceberg_maintenance
