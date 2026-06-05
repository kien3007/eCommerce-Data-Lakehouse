.PHONY: up down clickhouse-init spark-stream test clean

up: up-step-1 up-step-2 up-step-3 up-step-4 up-step-5 up-step-6
	@echo "All services started successfully."

up-step-1:
	docker compose up -d zookeeper kafka schema-registry kafka-ui minio minio-init clickhouse

up-step-2:
	docker compose up -d iceberg-rest

up-step-3:
	docker compose up -d spark-master spark-worker

up-step-4:
	docker compose up -d postgres redis airflow-init

up-step-5:
	docker compose up -d airflow-webserver airflow-scheduler airflow-worker

up-step-6:
	docker compose up -d trino superset

down:
	docker compose down -v

clickhouse-init:
	python clickhouse/init_clickhouse.py

spark-stream:
	docker exec -it spark-master /opt/spark/bin/spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-avro_2.12:3.5.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.iceberg:iceberg-aws-bundle:1.5.0 \
		--py-files /opt/spark_jobs/spark_utils.py \
		/opt/spark_jobs/streaming_to_bronze.py

produce:
	python ingestion/kafka_producer.py

test:
	pytest tests/ -v

clean:
	python -c "import shutil, pathlib; [shutil.rmtree(p) for p in pathlib.Path('.').rglob('__pycache__')]; [shutil.rmtree(p) for p in pathlib.Path('.').rglob('.pytest_cache')]"

