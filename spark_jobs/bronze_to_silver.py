import logging
from pyspark.sql.functions import col
from spark_utils import get_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform(bronze_logs_df, purchase_df):
    """
    Core transformation logic: enrich log data with cohort data and deduplicate.
    Extracted for unit testing.
    """
    # Extract only necessary cohort columns for enrichment
    cohort_df = purchase_df.select("user_id", "first_event_date", "start_of_week", "cohort_index_week").distinct()
    
    # Execute JOIN and Deduplication
    silver_df = bronze_logs_df.join(cohort_df, on="user_id", how="left")
    
    # Clean data & Deduplicate
    silver_df = silver_df.dropDuplicates(["event_time", "user_id", "product_id", "event_type"])

    return silver_df

def main():
    """
    Reads data from Bronze layer (Iceberg) and raw user cohort data from MinIO,
    performs a left join to enrich the data, deduplicates, and writes to Silver layer.
    """
    logger.info("Initializing SparkSession...")
    spark = get_spark_session("BronzeToSilver")

    logger.info("Reading Bronze table: lakehouse.bronze.log_tracking")
    bronze_logs_df = spark.table("lakehouse.bronze.log_tracking")
    
    logger.info("Reading cohort dataset from s3a://raw/purchase-behavior/02-purchase-behavior.csv")
    purchase_df = spark.read.csv("s3a://raw/purchase-behavior/02-purchase-behavior.csv", header=True, inferSchema=True)
    logger.info("Executing transformation (Enrichment and Deduplication)...")
    silver_df = transform(bronze_logs_df, purchase_df)
    
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    
    logger.info("Writing enriched data to lakehouse.silver.user_activity")
    silver_df.writeTo("lakehouse.silver.user_activity") \
        .tableProperty("format-version", "2") \
        .createOrReplace()
        
    logger.info("Bronze to Silver ETL process completed successfully.")

if __name__ == "__main__":
    main()
