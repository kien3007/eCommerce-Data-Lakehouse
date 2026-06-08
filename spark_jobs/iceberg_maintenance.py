import logging
from spark_utils import get_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def maintain_table(spark, table_name):
    """
    Runs Iceberg maintenance procedures on a given table.
    1. rewrite_data_files: Compacts small files into larger ones (default 128MB) to fix the 'small files problem'.
    2. expire_snapshots: Removes old snapshots and metadata files to save storage space.
    """
    logger.info(f"Starting maintenance for table: {table_name}")
    
    try:
        # 1. Compaction
        logger.info(f"[{table_name}] Compacting small data files...")
        spark.sql(f"CALL lakehouse.system.rewrite_data_files('{table_name}')").show()
        
        # 2. Expire Snapshots (retain last 5 days by default if configured, or uses Iceberg defaults)
        logger.info(f"[{table_name}] Expiring old snapshots...")
        spark.sql(f"CALL lakehouse.system.expire_snapshots('{table_name}')").show()
        
        # 3. Rewrite Manifests (optimize metadata)
        logger.info(f"[{table_name}] Rewriting manifests...")
        spark.sql(f"CALL lakehouse.system.rewrite_manifests('{table_name}')").show()
        
        logger.info(f"Successfully completed maintenance for {table_name}.")
    except Exception as e:
        logger.error(f"Failed to run maintenance on {table_name}: {e}")

def main():
    logger.info("Initializing SparkSession for Iceberg Maintenance...")
    spark = get_spark_session("IcebergMaintenance")
    
    # List of tables to maintain
    tables = [
        "bronze.log_tracking",
        "silver.users",
        "silver.products",
        "silver.categories",
        "silver.events"
    ]
    
    for table in tables:
        maintain_table(spark, table)

if __name__ == "__main__":
    main()
