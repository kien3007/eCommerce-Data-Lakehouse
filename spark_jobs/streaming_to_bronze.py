import logging
from pyspark.sql.functions import col, expr, to_timestamp
from pyspark.sql.avro.functions import from_avro
from spark_utils import get_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCHEMA_JSON = """
{
  "namespace": "com.lakehouse.logtracking",
  "name": "LogEvent",
  "type": "record",
  "fields": [
    {"name": "event_time", "type": ["null", "string"], "default": null},
    {"name": "event_type", "type": ["null", "string"], "default": null},
    {"name": "product_id", "type": ["null", "string"], "default": null},
    {"name": "category_id", "type": ["null", "string"], "default": null},
    {"name": "category_code", "type": ["null", "string"], "default": null},
    {"name": "brand", "type": ["null", "string"], "default": null},
    {"name": "price", "type": ["null", "float"], "default": null},
    {"name": "user_id", "type": ["null", "string"], "default": null},
    {"name": "user_session", "type": ["null", "string"], "default": null}
  ]
}
"""

def main():
    """
    Consumes Avro messages from Kafka, parses them, and writes them continuously 
    to the Iceberg Bronze layer.
    """
    logger.info("Initializing SparkSession with Iceberg and Kafka configurations.")
    spark = get_spark_session("KafkaToIcebergBronze")
    
    logger.info("Ensuring Iceberg Namespace and Bronze Table exist.")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    
    # CRITICAL: Do NOT drop the table in a streaming job to preserve historical data.
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.log_tracking (
            event_time TIMESTAMP,
            event_type STRING,
            product_id STRING,
            category_id STRING,
            category_code STRING,
            brand STRING,
            price FLOAT,
            user_id STRING,
            user_session STRING,
            event_date DATE
        )
        USING iceberg
        PARTITIONED BY (event_date)
    """)

    logger.info("Reading stream from Kafka topic 'log-tracking-raw'...")
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "log-tracking-raw") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 50000) \
        .option("failOnDataLoss", "false") \
        .load()

    # Decode Confluent Avro wire format (Magic byte + Schema ID -> 5 bytes offset)
    clean_df = raw_df.withColumn("avro_payload", expr("substring(value, 6, length(value)-5)"))
    
    parsed_df = clean_df.select(from_avro(col("avro_payload"), SCHEMA_JSON).alias("data")).select("data.*")
    
    parsed_df = parsed_df.withColumn(
        "event_time", 
        to_timestamp(expr("substring(event_time, 1, 19)"), "yyyy-MM-dd HH:mm:ss")
    ).withColumn(
        "event_date",
        expr("to_date(event_time)")
    )
    
    logger.info("Starting write stream to lakehouse.bronze.log_tracking (Iceberg)...")
    iceberg_query = parsed_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .option("path", "lakehouse.bronze.log_tracking") \
        .option("checkpointLocation", "/opt/spark_jobs/checkpoints/log_tracking/") \
        .start()
        
    logger.info("Starting live monitoring stream (Console)...")
    console_query = parsed_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .option("checkpointLocation", "/opt/spark_jobs/checkpoints/log_tracking_console/") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()