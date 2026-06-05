import os
import subprocess
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    template_path = 'clickhouse/init_tables.sql.template'
    
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        logger.warning("python-dotenv not installed. Relying on system environment variables.")

    # Read template
    if not os.path.exists(template_path):
        logger.error(f"Template file not found at {template_path}")
        return
        
    with open(template_path, 'r', encoding='utf-8') as f:
        sql_content = f.read()

    # Apply fail-fast credential substitution
    try:
        minio_user = os.environ['MINIO_ROOT_USER']
        minio_pass = os.environ['MINIO_ROOT_PASSWORD']
        ch_user = os.environ.get('CLICKHOUSE_USER', 'default')
        ch_pass = os.environ.get('CLICKHOUSE_PASSWORD', '')
    except KeyError as e:
        logger.error(f"Missing required environment variable: {e}")
        logger.error("Please ensure you have configured your .env file correctly.")
        exit(1)

    sql_content = sql_content.replace('${MINIO_ROOT_USER}', minio_user)
    sql_content = sql_content.replace('${MINIO_ROOT_PASSWORD}', minio_pass)
    sql_content = sql_content.replace('${CLICKHOUSE_USER}', ch_user)
    sql_content = sql_content.replace('${CLICKHOUSE_PASSWORD}', ch_pass)

    logger.info("Credentials injected securely into memory. Executing against ClickHouse...")

    # Run docker exec and pass the substituted SQL content via stdin
    cmd = ['docker', 'exec', '-i', 'clickhouse', 'clickhouse-client', '-n']
    
    process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = process.communicate(input=sql_content)

    if process.returncode != 0:
        logger.error(f"ClickHouse Initialization Failed!\n{stderr}")
        exit(process.returncode)
    else:
        logger.info("ClickHouse successfully initialized with Real-time tables, Dictionaries, and Views.")
        if stdout.strip():
            logger.info(f"Output: {stdout.strip()}")

if __name__ == '__main__':
    main()
