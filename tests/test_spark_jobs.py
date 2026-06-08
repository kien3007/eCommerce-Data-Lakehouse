import os
import sys
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import to_timestamp, col

# Add spark_jobs directory to path so we can import the scripts
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'spark_jobs'))
import bronze_to_silver
import silver_to_gold

@pytest.fixture(scope="session")
def spark():
    """
    Create a local SparkSession for testing without needing an actual cluster or Iceberg.
    """
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    
    spark = SparkSession.builder \
        .appName("PySpark Unit Tests") \
        .master("local[2]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()

def test_bronze_to_silver_transform(spark):
    """
    Test the Bronze to Silver transformation logic (3NF Decomposition).
    """
    # 1. Create mock Bronze data (simulate duplicated events & price changes)
    bronze_data = [
        ("2023-01-01 10:00:00", "view", "prod_1", "cat_1", "electronics.phone", "apple", 1000.0, "user_1", "session_1"),
        ("2023-01-01 10:00:00", "view", "prod_1", "cat_1", "electronics.phone", "apple", 1000.0, "user_1", "session_1"), # Duplicate
        ("2023-01-02 10:05:00", "cart", "prod_1", "cat_1", "electronics.phone", "apple", 900.0, "user_1", "session_1"), # Price drop!
    ]
    bronze_schema = ["event_time", "event_type", "product_id", "category_id", "category_code", "brand", "price", "user_id", "user_session"]
    bronze_df = spark.createDataFrame(bronze_data, schema=bronze_schema)
    bronze_df = bronze_df.withColumn("event_time", to_timestamp(col("event_time")))

    # 2. Create mock Cohort data (from MinIO raw CSV)
    cohort_data = [("user_1", "2023-01-01", "2023-01-01", 0)]
    cohort_schema = ["user_id", "first_event_date", "start_of_week", "cohort_index_week"]
    cohort_df = spark.createDataFrame(cohort_data, schema=cohort_schema)

    # 3. Run transformation
    silver_tables = bronze_to_silver.transform(bronze_df, cohort_df)
    
    # 4. Assertions
    assert "users" in silver_tables
    assert "categories" in silver_tables
    assert "products" in silver_tables
    assert "events" in silver_tables
    
    events_df = silver_tables["events"]
    # Count should be 2 instead of 3 due to exact duplicate deduplication
    assert events_df.count() == 2, "Deduplication failed in events table!"
    
    products_df = silver_tables["products"]
    # We should have 2 records for prod_1 since the price changed from 1000 to 900 (SCD2)
    assert products_df.count() == 2, "SCD Type 2 price tracking failed!"

def test_silver_to_gold_transform(spark):
    """
    Test the Silver to Gold transformation logic (Reconstruction and Analytics).
    """
    # 1. Create mock Silver data matching the 4 tables in 3NF
    events_data = [
        ("evt_1", "2023-11-01 10:00:00", "view", "apple_1", "user_1", "session_1", 1000.0),
        ("evt_2", "2023-11-01 10:05:00", "cart", "apple_1", "user_1", "session_1", 1000.0),
        ("evt_3", "2023-11-01 10:10:00", "purchase", "apple_1", "user_1", "session_1", 1000.0),
        ("evt_4", "2023-11-02 12:00:00", "view", "samsung_1", "user_2", "session_2", 500.0),
    ]
    events_df = spark.createDataFrame(events_data, schema=["event_id", "event_time", "event_type", "product_id", "user_id", "user_session", "sold_price"])
    events_df = events_df.withColumn("event_time", to_timestamp(col("event_time")))

    users_data = [
        ("user_1", "2023-11-01", "2023-10-30", 0),
        ("user_2", "2023-11-02", "2023-10-30", 0),
    ]
    users_df = spark.createDataFrame(users_data, schema=["user_id", "first_event_date", "start_of_week", "cohort_index_week"])

    products_data = [
        ("pk_1", "apple_1", "cat_1", "apple", 1000.0, "2023-01-01 00:00:00", None, True),
        ("pk_2", "samsung_1", "cat_1", "samsung", 500.0, "2023-01-01 00:00:00", "2024-01-01 00:00:00", False),
    ]
    products_df = spark.createDataFrame(products_data, schema=["product_key", "product_id", "category_id", "brand", "price", "effective_start_time", "effective_end_time", "is_current"])
    products_df = products_df.withColumn("effective_start_time", to_timestamp(col("effective_start_time")))

    categories_data = [
        ("cat_1", "electronics.phone", "electronics"),
    ]
    categories_df = spark.createDataFrame(categories_data, schema=["category_id", "category_code", "main_category"])

    # 2. Run transformation
    gold_tables = silver_to_gold.transform(events_df, users_df, products_df, categories_df)
    
    # 3. Assertions
    assert "sales_trend_daily" in gold_tables
    assert "cart_abandonment" in gold_tables
    assert "market_preferences" in gold_tables
    assert "rfm_segmentation" in gold_tables
    
    # Validate Sales Trend computation
    sales_df = gold_tables["sales_trend_daily"].collect()
    for row in sales_df:
        if str(row["date"]) == "2023-11-01":
            assert row["total_revenue"] == 1000.0
            assert row["total_purchases"] == 1
        elif str(row["date"]) == "2023-11-02":
            assert row["total_revenue"] == 0.0
            
    # Validate Cart Abandonment (user_1 bought, user_2 browsed)
    abandon_df = gold_tables["cart_abandonment"].collect()
    for row in abandon_df:
        if row["user_id"] == "user_1":
            assert row["status"] == "Purchased"
        elif row["user_id"] == "user_2":
            assert row["status"] == "Browsing Only"
