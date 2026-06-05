import logging
from pyspark.sql.functions import (
    count, countDistinct, sum, when, col, to_date, current_date,
    datediff, date_trunc, max, min, split, lit, collect_set, size, hour, dayofweek, concat_ws
)
from spark_utils import get_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform(silver_df):
    """
    Core transformation logic: Generates 9 gold analytics DataFrames from Silver data.
    Returns a dictionary mapping table names to DataFrames.
    """
    gold_tables = {}
    
    # Prepare time columns
    silver_df = silver_df.withColumn("date", to_date(col("event_time"))) \
                         .withColumn("interaction_week", date_trunc("week", col("event_time")))

    # 1. Sales Trend
    gold_tables["sales_trend_daily"] = silver_df.groupBy("date").agg(
        count(when(col("event_type") == "view", True)).alias("total_views"),
        count(when(col("event_type") == "cart", True)).alias("total_carts"),
        count(when(col("event_type") == "purchase", True)).alias("total_purchases"),
        sum(when(col("event_type") == "purchase", col("price")).otherwise(0)).alias("total_revenue")
    )

    # 2. Weekly Cohort Retention
    cohort_df = silver_df.filter(col("first_event_date").isNotNull())
    cohort_df = cohort_df.withColumn(
        "weeks_after", 
        (datediff(col("interaction_week"), date_trunc("week", to_date(col("first_event_date")))) / 7).cast("int")
    )
    gold_tables["weekly_cohort_retention"] = cohort_df.groupBy("start_of_week", "weeks_after").agg(
        countDistinct("user_id").alias("retained_users")
    )

    # 3. Market Preferences
    gold_tables["market_preferences"] = silver_df.filter(col("brand").isin("apple", "samsung", "xiaomi")).groupBy("brand").agg(
        count(when(col("event_type") == "view", True)).alias("total_views"),
        count(when(col("event_type") == "purchase", True)).alias("total_purchases"),
        sum(when(col("event_type") == "purchase", col("price")).otherwise(0)).alias("total_revenue")
    ).withColumn("conversion_rate", col("total_purchases") / col("total_views"))

    # 4. RFM Segmentation
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

    # 5. Cart Abandonment Rate
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

    # 6. Session Engagement
    gold_tables["session_engagement"] = silver_df.filter(col("user_session").isNotNull()).groupBy("user_session").agg(
        count("*").alias("events_count"),
        count(when(col("event_type") == "purchase", True)).alias("purchases_count"),
        min("event_time").alias("start_time"),
        max("event_time").alias("end_time")
    ).withColumn(
        "duration_seconds", 
        col("end_time").cast("long") - col("start_time").cast("long")
    )

    # 7. Time of Day Trends
    time_df = silver_df.withColumn("hour", hour("event_time")) \
                       .withColumn("day_of_week", dayofweek("event_time"))
                       
    gold_tables["time_of_day_trends"] = time_df.groupBy("day_of_week", "hour").agg(
        count(when(col("event_type") == "view", True)).alias("total_views"),
        count(when(col("event_type") == "purchase", True)).alias("total_purchases")
    )

    # 8. Market Basket Analysis
    category_df = silver_df.filter(col("event_type") == "purchase").filter(col("category_code").isNotNull()).withColumn(
        "main_category", split(col("category_code"), "\.")[0]
    )
    basket_df = category_df.groupBy("user_session").agg(
        collect_set("main_category").alias("categories_bought")
    ).filter(size("categories_bought") > 1)
    
    gold_tables["market_basket"] = basket_df.withColumn("basket", concat_ws(", ", "categories_bought")).drop("categories_bought")

    # 9. Category Performance
    gold_tables["category_performance"] = silver_df.filter(col("category_code").isNotNull()).withColumn(
        "main_category", split(col("category_code"), "\.")[0]
    ).groupBy("main_category").agg(
        count(when(col("event_type") == "view", True)).alias("views"),
        count(when(col("event_type") == "cart", True)).alias("carts"),
        count(when(col("event_type") == "purchase", True)).alias("purchases"),
        sum(when(col("event_type") == "purchase", col("price")).otherwise(0)).alias("revenue")
    )

    return gold_tables

def main():
    """
    Reads from the Silver layer and generates 9 specialized Gold tables 
    for advanced analytics dashboards (e.g., RFM, Cohort, Funnel).
    """
    logger.info("Initializing SparkSession...")
    spark = get_spark_session("SilverToGold_AdvancedAnalytics")

    logger.info("Reading Silver table: lakehouse.silver.user_activity")
    silver_df = spark.table("lakehouse.silver.user_activity")
    
    logger.info("Executing transformations...")
    gold_tables = transform(silver_df)
    
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    
    for table_name, df in gold_tables.items():
        logger.info(f"Writing table: lakehouse.gold.{table_name}")
        df.writeTo(f"lakehouse.gold.{table_name}").tableProperty("format-version", "2").createOrReplace()

    logger.info("All 9 Gold Analytics tables have been successfully generated.")

if __name__ == "__main__":
    main()
