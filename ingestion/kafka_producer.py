import csv
import time
import argparse
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
import logging
from confluent_kafka.schema_registry.avro import AvroSerializer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Avro Schema definition for LogEvent
SCHEMA_STR = """
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

def delivery_report(err, msg):
    """Callback triggered on successful/failed delivery."""
    if err is not None:
        logger.error(f"Delivery failed for record {msg.key()}: {err}")

def main():
    parser = argparse.ArgumentParser(description="Kafka Avro Producer for Log Tracking")
    parser.add_argument('--file', type=str, default='dataset/01-log-tracking.csv', help='Path to CSV file')
    parser.add_argument('--topic', type=str, default='log-tracking-raw', help='Kafka Topic')
    parser.add_argument('--limit', type=int, default=-1, help='Max lines to send (-1 for all)')
    parser.add_argument('--rate', type=float, default=0.01, help='Delay between messages (seconds)')
    args = parser.parse_args()

    # 1. Configure Schema Registry
    schema_registry_conf = {'url': 'http://localhost:8081'}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    avro_serializer = AvroSerializer(schema_registry_client, SCHEMA_STR)

    # 2. Configure Kafka Producer
    producer_conf = {
        'bootstrap.servers': 'localhost:9092',
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_serializer,
        'linger.ms': 100,           # Optimize for batching
        'batch.num.messages': 1000  # Batch size

    }
    producer = SerializingProducer(producer_conf)

    logger.info(f"Starting to stream data from {args.file} to topic {args.topic}")
    count = 0

    # 3. Read CSV iteratively to preserve memory
    with open(args.file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if args.limit != -1 and count >= args.limit:
                break
            
            # Data pre-processing (cast price to float)
            record = dict(row)
            if not record.get('price'):
                record['price'] = None
            else:
                try:
                    record['price'] = float(record['price'])
                except ValueError:
                    record['price'] = None
            
            # Partition by user_id to ensure chronological ordering per user
            key = record.get('user_id', 'unknown_user')
            
            try:
                producer.produce(
                    topic=args.topic,
                    key=key,
                    value=record,
                    on_delivery=delivery_report
                )
            except BufferError:
                logger.warning("Local producer buffer full, waiting...")
                producer.poll(1)
                producer.produce(topic=args.topic, key=key, value=record, on_delivery=delivery_report)

            count += 1
            if count % 5000 == 0:
                logger.info(f"Produced {count} messages...")
                producer.poll(0) # Maintain callbacks
            
            if args.rate > 0:
                time.sleep(args.rate)

    logger.info("Flushing remaining records...")
    producer.flush()
    logger.info(f"Completed. Total messages produced: {count}")

if __name__ == '__main__':
    main()
