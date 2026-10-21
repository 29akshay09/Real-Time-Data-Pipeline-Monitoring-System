import time
from metrics import start_metrics_server
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    coalesce,
    lit
)
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    IntegerType,
    DoubleType,
    StringType
)

from metrics import (
    start_metrics_server,
    record_success,
    record_failure
)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "pipeline-events"

# Define incoming JSON structure
event_schema = StructType([
    StructField("event_id", LongType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("event_timestamp", StringType(), True)
])


def process_batch(batch_df, batch_id):
    """Validate a micro-batch and update Prometheus metrics."""

    start_time = time.time()

    valid_condition = (
        col("event_id").isNotNull()
        & col("customer_id").isNotNull()
        & col("amount").isNotNull()
        & (col("amount") > 0)
        & col("status").isin("SUCCESS", "FAILED")
    )

    # Treat null validation results as invalid.
    valid_df = batch_df.filter(
        coalesce(valid_condition, lit(False))
    )

    invalid_df = batch_df.filter(
        ~coalesce(valid_condition, lit(False))
    )

    # Count records in each category.
    valid_count = valid_df.count()
    invalid_count = invalid_df.count()

    # Update metrics once per batch, not once per record.
    elapsed = time.time() - start_time

    if valid_count > 0:
        record_success(elapsed, count=valid_count)

    if invalid_count > 0:
        record_failure(count=invalid_count)

    print(
        f"Batch ID: {batch_id} | "
        f"Valid: {valid_count} | "
        f"Invalid: {invalid_count} | "
        f"Processing time: {elapsed:.3f}s"
    )

    # Display records for verification.
    print("Valid records:")
    valid_df.show(truncate=False)

    print("Invalid records:")
    invalid_df.show(truncate=False)


def main():
    # Start the Prometheus metrics endpoint.
    start_metrics_server(port=8000)

    spark = (
        SparkSession.builder
        .appName("RealTimeDataPipelineMonitoring")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("Starting Spark Structured Streaming...")
    print(f"Kafka topic: {KAFKA_TOPIC}")

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_stream = (
        kafka_stream
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(
            from_json(col("json_value"), event_schema).alias("event")
        )
        .select("event.*")
        .withColumn("processing_timestamp", current_timestamp())
    )

    query = (
        parsed_stream.writeStream
        .outputMode("append")
        .foreachBatch(process_batch)
        .option(
            "checkpointLocation",
            "/tmp/spark-checkpoints/monitoring"
        )
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()