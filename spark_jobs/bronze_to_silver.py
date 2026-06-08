import logging
from pyspark.sql.functions import col, split, lag, lead, md5, concat_ws
from pyspark.sql.window import Window
from spark_utils import get_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def build_users_dim(bronze_logs_df, purchase_df):
    cohort_df = purchase_df.select("user_id", "first_event_date", "start_of_week", "cohort_index_week").distinct()
    users_df = bronze_logs_df.select("user_id").distinct().join(cohort_df, on="user_id", how="left")
    return users_df

def build_categories_dim(bronze_logs_df):
    categories_df = bronze_logs_df.filter(col("category_code").isNotNull()) \
        .select("category_id", "category_code").distinct() \
        .withColumn("main_category", split(col("category_code"), "\.")[0])
    return categories_df

def build_products_scd2(bronze_logs_df, spark=None):
    # Get distinct price points chronologically
    product_prices = bronze_logs_df.filter(col("product_id").isNotNull()) \
        .select("product_id", "category_id", "brand", "price", "event_time") \
        .dropDuplicates(["product_id", "price", "event_time"])

    # If doing incremental load, we must mix the currently active prices to correctly detect boundary price changes
    if spark and spark.catalog.tableExists("lakehouse.silver.products"):
        active_products = spark.table("lakehouse.silver.products") \
            .filter("is_current = true") \
            .select("product_id", "category_id", "brand", "price", col("effective_start_time").alias("event_time"))
        product_prices = product_prices.unionByName(active_products).dropDuplicates(["product_id", "price", "event_time"])

    windowSpec = Window.partitionBy("product_id").orderBy("event_time")

    # Filter only rows where the price changed compared to the previous row
    products_scd = product_prices.withColumn("prev_price", lag("price").over(windowSpec)) \
        .filter((col("prev_price").isNull()) | (col("price") != col("prev_price"))) \
        .drop("prev_price")

    # Calculate SCD Type 2 fields
    products_scd = products_scd.withColumn("effective_start_time", col("event_time")) \
        .withColumn("effective_end_time", lead("event_time").over(windowSpec)) \
        .withColumn("is_current", col("effective_end_time").isNull()) \
        .drop("event_time")

    # Generate a surrogate key for the dimension
    products_scd = products_scd.withColumn(
        "product_key", 
        md5(concat_ws("_", col("product_id"), col("effective_start_time").cast("string")))
    )
    return products_scd

def build_events_fact(bronze_logs_df):
    events_df = bronze_logs_df \
        .withColumn("event_id", md5(concat_ws("_", col("user_id"), col("event_time").cast("string"), col("event_type"), col("product_id")))) \
        .select(
            "event_id", 
            "event_time", 
            "event_type", 
            "product_id", 
            "user_id", 
            "user_session", 
            col("price").alias("sold_price")
        ).dropDuplicates(["event_id"])
    return events_df

def transform(bronze_logs_df, purchase_df, spark=None):
    """Entry point for testing the 4 transformations."""
    return {
        "users": build_users_dim(bronze_logs_df, purchase_df),
        "categories": build_categories_dim(bronze_logs_df),
        "products": build_products_scd2(bronze_logs_df, spark),
        "events": build_events_fact(bronze_logs_df)
    }

def main():
    logger.info("Initializing SparkSession...")
    spark = get_spark_session("BronzeToSilver_3NF")

    logger.info("Reading Bronze table: lakehouse.bronze.log_tracking")
    bronze_logs_df = spark.table("lakehouse.bronze.log_tracking")
    
    logger.info("Reading cohort dataset from s3a://raw/purchase-behavior/02-purchase-behavior.csv")
    purchase_df = spark.read.csv("s3a://raw/purchase-behavior/02-purchase-behavior.csv", header=True)
    purchase_df = purchase_df.select(
        col("user_id"),
        col("first_event_date"),
        col("start_of_week"),
        col("cohort_index_week").cast("int")
    )
    
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    
    # Check if this is an Incremental Load (tables exist) or Initial Load
    is_incremental = spark.catalog.tableExists("lakehouse.silver.users")
    
    if is_incremental:
        logger.info("Silver tables exist. Initiating INCREMENTAL LOAD...")
        # Read only new data (e.g. from the last 2 days to be safe with late arrivals)
        bronze_logs_df = bronze_logs_df.filter("event_time >= current_date() - interval 2 days")
    else:
        logger.info("Silver tables do NOT exist. Initiating INITIAL FULL LOAD...")

    logger.info("Executing 3NF transformations...")
    silver_tables = transform(bronze_logs_df, purchase_df, spark)
    
    for table_name, df in silver_tables.items():
        if not is_incremental:
            logger.info(f"Initial Load: Creating lakehouse.silver.{table_name}")
            df.writeTo(f"lakehouse.silver.{table_name}") \
                .tableProperty("format-version", "2") \
                .createOrReplace()
        else:
            logger.info(f"Incremental Load: Merging into lakehouse.silver.{table_name}")
            # Register new data as a temp view to run MERGE SQL
            df.createOrReplaceTempView(f"new_{table_name}")
            
            if table_name == "users":
                spark.sql(f"""
                    MERGE INTO lakehouse.silver.users t
                    USING new_users s ON t.user_id = s.user_id
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                """)
            elif table_name == "categories":
                spark.sql(f"""
                    MERGE INTO lakehouse.silver.categories t
                    USING new_categories s ON t.category_id = s.category_id
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                """)
            elif table_name == "events":
                spark.sql(f"""
                    MERGE INTO lakehouse.silver.events t
                    USING new_events s ON t.event_id = s.event_id
                    WHEN NOT MATCHED THEN INSERT *
                """)
            elif table_name == "products":
                # For SCD2: Update old active records if closed, and insert new records.
                spark.sql(f"""
                    MERGE INTO lakehouse.silver.products t
                    USING new_products s ON t.product_key = s.product_key
                    WHEN MATCHED AND (t.is_current != s.is_current OR t.effective_end_time != s.effective_end_time) THEN 
                        UPDATE SET effective_end_time = s.effective_end_time, is_current = s.is_current
                    WHEN NOT MATCHED THEN 
                        INSERT *
                """)
            
    logger.info("Bronze to Silver 3NF ETL process completed successfully.")

if __name__ == "__main__":
    main()
