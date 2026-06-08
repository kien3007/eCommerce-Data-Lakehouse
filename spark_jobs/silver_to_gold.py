import logging
from pyspark.sql.functions import (
    count, countDistinct, sum, when, col, to_date, current_date,
    datediff, date_trunc, max, min, split, lit, collect_set, size, hour, dayofweek, concat_ws
)
from spark_utils import get_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def transform_time_series(silver_df):
    """
    Computes time-series models. These are safe to run on an incremental subset 
    of data (e.g. last 2 days) and MERGE idempotently by Date/Time.
    """
    gold_tables = {}
    
    # 1. Sales Trend Daily
    gold_tables["sales_trend_daily"] = silver_df.groupBy("date").agg(
        count(when(col("event_type") == "view", True)).alias("total_views"),
        count(when(col("event_type") == "cart", True)).alias("total_carts"),
        count(when(col("event_type") == "purchase", True)).alias("total_purchases"),
        sum(when(col("event_type") == "purchase", col("price")).otherwise(0)).alias("total_revenue")
    )

    # 2. Time of Day Trends
    time_df = silver_df.withColumn("hour", hour("event_time")) \
                       .withColumn("day_of_week", dayofweek("event_time"))
                       
    gold_tables["time_of_day_trends"] = time_df.groupBy("date", "day_of_week", "hour").agg(
        count(when(col("event_type") == "view", True)).alias("total_views"),
        count(when(col("event_type") == "purchase", True)).alias("total_purchases")
    )
    
    return gold_tables

def transform_stateful(silver_df):
    """
    Computes stateful and cumulative models. These require the FULL history to be 
    accurate (e.g. RFM, Lifetime value). We will overwrite these completely.
    """
    gold_tables = {}
    
    # 1. Weekly Cohort Retention
    cohort_df = silver_df.filter(col("first_event_date").isNotNull())
    cohort_df = cohort_df.withColumn(
        "weeks_after", 
        (datediff(col("interaction_week"), date_trunc("week", to_date(col("first_event_date")))) / 7).cast("int")
    )
    gold_tables["weekly_cohort_retention"] = cohort_df.groupBy("start_of_week", "weeks_after").agg(
        countDistinct("user_id").alias("retained_users")
    )

    # 2. Market Preferences
    gold_tables["market_preferences"] = silver_df.filter(col("brand").isin("apple", "samsung", "xiaomi")).groupBy("brand").agg(
        count(when(col("event_type") == "view", True)).alias("total_views"),
        count(when(col("event_type") == "purchase", True)).alias("total_purchases"),
        sum(when(col("event_type") == "purchase", col("price")).otherwise(0)).alias("total_revenue")
    ).withColumn("conversion_rate", col("total_purchases") / col("total_views"))

    # 3. RFM Segmentation
    ref_date = current_date()
    rfm_df = silver_df.filter(col("event_type") == "purchase").groupBy("user_id").agg(
        datediff(ref_date, max("date")).alias("recency"),
        countDistinct("event_time").alias("frequency"),
        sum("price").alias("monetary")
    )
    gold_tables["rfm_segmentation"] = rfm_df.withColumn(
        "segment",
        when((col("recency") <= 15) & (col("frequency") >= 3), "Champions")
        .when((col("recency") <= 30) & (col("frequency") >= 2), "Loyal Customers")
        .when((col("recency") > 30) & (col("frequency") > 1), "At Risk")
        .otherwise("Lost Customers")
    )

    # 4. Cart Abandonment Rate
    abandon_df = silver_df.groupBy("user_id").agg(
        count(when(col("event_type") == "cart", True)).alias("total_carts"),
        count(when(col("event_type") == "purchase", True)).alias("total_purchases")
    )
    gold_tables["cart_abandonment"] = abandon_df.withColumn(
        "status",
        when((col("total_carts") > 0) & (col("total_purchases") == 0), "Abandoned")
        .when(col("total_purchases") > 0, "Purchased")
        .otherwise("Browsing Only")
    )

    # 5. Session Engagement
    gold_tables["session_engagement"] = silver_df.filter(col("user_session").isNotNull()).groupBy("user_session").agg(
        count("*").alias("events_count"),
        count(when(col("event_type") == "purchase", True)).alias("purchases_count"),
        min("event_time").alias("start_time"),
        max("event_time").alias("end_time")
    ).withColumn(
        "duration_seconds", 
        col("end_time").cast("long") - col("start_time").cast("long")
    )

    # 6. Market Basket Analysis
    category_df_joined = silver_df.filter(col("event_type") == "purchase").filter(col("category_code").isNotNull()).withColumn(
        "main_category", split(col("category_code"), "\.")[0]
    )
    basket_df = category_df_joined.groupBy("user_session").agg(
        collect_set("main_category").alias("categories_bought")
    ).filter(size("categories_bought") > 1)
    
    gold_tables["market_basket"] = basket_df.withColumn("basket", concat_ws(", ", "categories_bought")).drop("categories_bought")

    # 7. Category Performance
    gold_tables["category_performance"] = silver_df.filter(col("category_code").isNotNull()).withColumn(
        "main_category", split(col("category_code"), "\.")[0]
    ).groupBy("main_category").agg(
        count(when(col("event_type") == "view", True)).alias("views"),
        count(when(col("event_type") == "cart", True)).alias("carts"),
        count(when(col("event_type") == "purchase", True)).alias("purchases"),
        sum(when(col("event_type") == "purchase", col("price")).otherwise(0)).alias("revenue")
    )

    return gold_tables

def transform(events_df, users_df, products_df, categories_df, is_incremental=False):
    """
    Main transform entry point. Handles both full and incremental logic.
    For unit tests, is_incremental is False.
    """
    # 1. Denormalize
    silver_df = events_df.join(users_df, on="user_id", how="left")
    silver_df = silver_df.join(
        products_df,
        (events_df.product_id == products_df.product_id) &
        (events_df.event_time >= products_df.effective_start_time) &
        ((products_df.effective_end_time.isNull()) | (events_df.event_time < products_df.effective_end_time)),
        how="left"
    ).drop(products_df.product_id)
    silver_df = silver_df.join(categories_df, on="category_id", how="left")
    
    silver_df = silver_df.withColumn("price", col("sold_price"))
    silver_df = silver_df.withColumn("date", to_date(col("event_time"))) \
                         .withColumn("interaction_week", date_trunc("week", col("event_time")))

    time_series_tables = transform_time_series(silver_df)
    stateful_tables = transform_stateful(silver_df)
    
    return {**time_series_tables, **stateful_tables}


def main():
    logger.info("Initializing SparkSession...")
    spark = get_spark_session("SilverToGold_AdvancedAnalytics")

    logger.info("Reading Silver tables (3NF)...")
    events_df = spark.table("lakehouse.silver.events")
    users_df = spark.table("lakehouse.silver.users")
    products_df = spark.table("lakehouse.silver.products")
    categories_df = spark.table("lakehouse.silver.categories")
    
    is_incremental = spark.catalog.tableExists("lakehouse.gold.sales_trend_daily")

    if is_incremental:
        logger.info("Gold tables exist. Using HYBRID INCREMENTAL approach.")
        # For Time-series, we only need the last 2 days
        recent_events_df = events_df.filter("event_time >= current_date() - interval 2 days")
        
        # 1. Process Time-Series Incremental
        logger.info("Calculating Time-Series Incremental updates...")
        ts_flat_df = recent_events_df.join(users_df, on="user_id", how="left") \
            .join(products_df, (recent_events_df.product_id == products_df.product_id) & (recent_events_df.event_time >= products_df.effective_start_time) & ((products_df.effective_end_time.isNull()) | (recent_events_df.event_time < products_df.effective_end_time)), how="left").drop(products_df.product_id) \
            .join(categories_df, on="category_id", how="left") \
            .withColumn("price", col("sold_price")) \
            .withColumn("date", to_date(col("event_time")))
            
        time_series_tables = transform_time_series(ts_flat_df)
        
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
        for table_name, df in time_series_tables.items():
            logger.info(f"Merging Incremental Time-Series: {table_name}")
            df.createOrReplaceTempView(f"new_{table_name}")
            if table_name == "sales_trend_daily":
                spark.sql(f"""
                    MERGE INTO lakehouse.gold.sales_trend_daily t
                    USING new_sales_trend_daily s ON t.date = s.date
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                """)
            elif table_name == "time_of_day_trends":
                spark.sql(f"""
                    MERGE INTO lakehouse.gold.time_of_day_trends t
                    USING new_time_of_day_trends s ON t.date = s.date AND t.day_of_week = s.day_of_week AND t.hour = s.hour
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                """)
        
        # 2. Process Stateful Full-Refresh
        logger.info("Calculating Stateful Full-Refresh updates...")
        full_flat_df = events_df.join(users_df, on="user_id", how="left") \
            .join(products_df, (events_df.product_id == products_df.product_id) & (events_df.event_time >= products_df.effective_start_time) & ((products_df.effective_end_time.isNull()) | (events_df.event_time < products_df.effective_end_time)), how="left").drop(products_df.product_id) \
            .join(categories_df, on="category_id", how="left") \
            .withColumn("price", col("sold_price")) \
            .withColumn("date", to_date(col("event_time"))) \
            .withColumn("interaction_week", date_trunc("week", col("event_time")))
            
        stateful_tables = transform_stateful(full_flat_df)
        
        for table_name, df in stateful_tables.items():
            logger.info(f"Overwriting Stateful Table: {table_name}")
            df.writeTo(f"lakehouse.gold.{table_name}").tableProperty("format-version", "2").createOrReplace()
            
    else:
        logger.info("Gold tables do not exist. Performing FULL INITIAL LOAD.")
        gold_tables = transform(events_df, users_df, products_df, categories_df, False)
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
        for table_name, df in gold_tables.items():
            logger.info(f"Creating Initial Table: lakehouse.gold.{table_name}")
            df.writeTo(f"lakehouse.gold.{table_name}").tableProperty("format-version", "2").createOrReplace()

    logger.info("Silver to Gold Hybrid ETL process completed successfully.")

if __name__ == "__main__":
    main()
