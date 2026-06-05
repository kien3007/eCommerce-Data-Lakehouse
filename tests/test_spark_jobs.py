import os
import sys
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType

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
    Test the Bronze to Silver transformation logic (Enrichment and Deduplication).
    """
    # 1. Create mock Bronze data (simulate duplicated events)
    bronze_data = [
        ("2023-01-01 10:00:00", "view", "prod_1", "cat_1", "electronics.phone", "apple", 1000.0, "user_1", "session_1"),
        ("2023-01-01 10:00:00", "view", "prod_1", "cat_1", "electronics.phone", "apple", 1000.0, "user_1", "session_1"), # Duplicate
        ("2023-01-01 10:05:00", "cart", "prod_1", "cat_1", "electronics.phone", "apple", 1000.0, "user_1", "session_1"),
    ]
    bronze_schema = ["event_time", "event_type", "product_id", "category_id", "category_code", "brand", "price", "user_id", "user_session"]
    bronze_df = spark.createDataFrame(bronze_data, schema=bronze_schema)

    # 2. Create mock Cohort data (from MinIO raw CSV)
    cohort_data = [
        ("user_1", "2023-01-01", "2023-01-01", 0),
        ("user_2", "2023-01-02", "2023-01-02", 0)
    ]
    cohort_schema = ["user_id", "first_event_date", "start_of_week", "cohort_index_week"]
    cohort_df = spark.createDataFrame(cohort_data, schema=cohort_schema)

    # 3. Run transformation
    silver_df = bronze_to_silver.transform(bronze_df, cohort_df)
    
    # 4. Assertions
    # Count should be 2 instead of 3 due to deduplication of the exact same event
    assert silver_df.count() == 2, "Deduplication failed!"
    
    # Check if cohort data was successfully joined
    user_1_records = silver_df.filter(silver_df.user_id == "user_1").collect()
    assert user_1_records[0]["cohort_index_week"] == 0, "Enrichment join failed!"

def test_silver_to_gold_transform(spark):
    """
    Test the Silver to Gold transformation logic (Advanced Analytics).
    """
    # 1. Create mock Silver data
    silver_data = [
        ("2023-11-01 10:00:00", "view", "apple", 1000.0, "user_1", "session_1", "2023-11-01", "2023-10-30", 0),
        ("2023-11-01 10:05:00", "cart", "apple", 1000.0, "user_1", "session_1", "2023-11-01", "2023-10-30", 0),
        ("2023-11-01 10:10:00", "purchase", "apple", 1000.0, "user_1", "session_1", "2023-11-01", "2023-10-30", 0),
        ("2023-11-02 12:00:00", "view", "samsung", 500.0, "user_2", "session_2", "2023-11-02", "2023-10-30", 0),
    ]
    # In silver, the date was added, but the transform function adds it. 
    # Wait, transform() adds 'date' and 'interaction_week'.
    silver_schema = ["event_time", "event_type", "brand", "price", "user_id", "user_session", "first_event_date", "start_of_week", "cohort_index_week"]
    silver_df = spark.createDataFrame(silver_data, schema=silver_schema)
    
    # Mock event_time as timestamp because transform expects it
    from pyspark.sql.functions import to_timestamp, col
    silver_df = silver_df.withColumn("event_time", to_timestamp(col("event_time")))
    
    # We also need category_code for Market Basket/Category Performance to not fail
    silver_df = silver_df.withColumn("category_code", col("brand")) # Fake it for test

    # 2. Run transformation
    gold_tables = silver_to_gold.transform(silver_df)
    
    # 3. Assertions
    assert "sales_trend_daily" in gold_tables
    assert "cart_abandonment" in gold_tables
    
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
