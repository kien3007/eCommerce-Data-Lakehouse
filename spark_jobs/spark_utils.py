from pyspark.sql import SparkSession
import logging
import os

logger = logging.getLogger(__name__)

def get_spark_session(app_name: str) -> SparkSession:
    """
    Creates and returns a SparkSession with standard configurations
    for Iceberg Lakehouse and MinIO (S3).
    Credentials are read from environment variables (injected via docker-compose env_file).
    """
    logger.info(f"Initializing SparkSession for app: {app_name}...")

    s3_access_key = os.environ["AWS_ACCESS_KEY_ID"]
    s3_secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    s3_endpoint = os.environ.get("S3_ENDPOINT", "http://minio:9000") # Endpoint is safe to default
    s3_region = os.environ.get("AWS_REGION", "us-east-1")

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "rest") \
        .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181") \
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.lakehouse.s3.endpoint", s3_endpoint) \
        .config("spark.sql.catalog.lakehouse.s3.access-key-id", s3_access_key) \
        .config("spark.sql.catalog.lakehouse.s3.secret-access-key", s3_secret_key) \
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true") \
        .config("spark.sql.catalog.lakehouse.client.region", s3_region) \
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    return spark

