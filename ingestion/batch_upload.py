import os
import logging
import boto3
from botocore.exceptions import NoCredentialsError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """
    Standalone script to upload static CSV datasets to the MinIO raw layer.
    Useful for local testing without Airflow.
    """
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        logger.warning("python-dotenv not installed. Relying on system env vars.")

    endpoint = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000') # Localhost is safe to default for testing
    access_key = os.environ['MINIO_ROOT_USER']
    secret_key = os.environ['MINIO_ROOT_PASSWORD']
    
    bucket_name = 'raw'
    
    # Resolve absolute path based on script location
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    file_path = os.path.join(base_dir, 'dataset', '02-purchase-behavior.csv')
    
    object_name = 'purchase-behavior/02-purchase-behavior.csv'

    logger.info("Initializing S3 Client for MinIO...")
    s3_client = boto3.client('s3',
                             endpoint_url=endpoint,
                             aws_access_key_id=access_key,
                             aws_secret_access_key=secret_key)

    logger.info(f"Uploading {file_path} to s3://{bucket_name}/{object_name}")
    
    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        logger.info("Upload completed successfully!")
    except FileNotFoundError:
        logger.error("Error: CSV file not found. Please check the dataset/ directory.")
    except NoCredentialsError:
        logger.error("Error: MinIO credentials not found.")
    except Exception as e:
        logger.error(f"System error occurred: {e}")

if __name__ == "__main__":
    main()
