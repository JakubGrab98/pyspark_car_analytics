#!/bin/bash

if [ -d "$MINIO_DATA_DIR" ]; then
    echo "Starting MinIO server..."
    minio server $MINIO_DATA_DIR --console-address ":9001" &
fi

if [ "$SPARK_MODE" = "master" ]; then
    echo "Starting Spark Master..."
    start-master.sh
elif [ "$SPARK_MODE" = "worker" ]; then
    echo "Starting Spark Worker..."
    start-worker.sh "$SPARK_MASTER_URL"
else
    echo "Unknown SPARK_MODE: $SPARK_MODE"
    exit 1
fi

tail -f /dev/null
