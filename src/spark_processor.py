"""
Spark Structured Streaming processor for Bank X fraud detection (TP5).
Single Kafka consumer + foreachBatch for reliable metrics output.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    approx_count_distinct,
    avg,
    col,
    count,
    current_timestamp,
    expr,
    from_json,
    lit,
    sum as spark_sum,
    to_timestamp,
    window,
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "bank-transactions")
OUTPUT_BASE = os.getenv("OUTPUT_BASE", "/workspace/data/output")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", "/workspace/checkpoints")
TRIGGER = os.getenv("TRIGGER_INTERVAL", "10 seconds")
HISTORY_PATH = f"{OUTPUT_BASE}/tx_history"

TX_SCHEMA = StructType(
    [
        StructField("msg_entity", StringType()),
        StructField("app_type", StringType()),
        StructField("send_entity", StringType()),
        StructField("receive_entity", StringType()),
        StructField("send_id", StringType()),
        StructField("receive_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("date", StringType()),
        StructField("tx_type", StringType()),
        StructField("tx_id", StringType()),
    ]
)

WINDOW_SPECS = [
    ("3h", "3 hours", "30 minutes"),
    ("7d", "7 days", "1 hour"),
    ("3w", "21 days", "6 hours"),
    ("3mo", "90 days", "1 day"),
]


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("TP5-Fraud-Processor")
        .master(os.getenv("SPARK_MASTER", "spark://spark-master:7077"))
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.kafka:kafka-clients:3.5.1",
        )
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )


def parse_batch(batch_df: DataFrame) -> DataFrame:
    return (
        batch_df.select(from_json(col("value").cast("string"), TX_SCHEMA).alias("tx"))
        .select("tx.*")
        .withColumn("event_time", to_timestamp(col("date")))
        .filter(col("send_id").isNotNull() & col("receive_id").isNotNull() & col("amount").isNotNull())
    )


def user_perspective(df: DataFrame) -> DataFrame:
    sent = df.select(
        col("send_id").alias("user_id"),
        col("event_time"),
        col("amount"),
        col("receive_id").alias("counterparty_id"),
        lit("sent").alias("direction"),
        col("tx_type"),
        col("tx_id"),
    )
    received = df.select(
        col("receive_id").alias("user_id"),
        col("event_time"),
        col("amount"),
        col("send_id").alias("counterparty_id"),
        lit("received").alias("direction"),
        col("tx_type"),
        col("tx_id"),
    )
    return sent.unionByName(received)


def write_windowed(spark: SparkSession, history: DataFrame, label: str, duration: str, slide: str) -> None:
    user_df = user_perspective(history)
    agg = (
        user_df.groupBy(window(col("event_time"), duration, slide), col("user_id"), col("direction"))
        .agg(
            avg("amount").alias("avg_amount"),
            count("*").alias("tx_count"),
            spark_sum("amount").alias("total_amount"),
            approx_count_distinct("counterparty_id").alias("distinct_counterparties"),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .withColumn("processed_at", current_timestamp())
        .drop("window")
    )
    agg.coalesce(1).write.mode("overwrite").parquet(f"{OUTPUT_BASE}/windowed_{label}")


def write_lifetime(history: DataFrame) -> None:
    user_df = user_perspective(history)
    lifetime = (
        user_df.groupBy("user_id", "direction")
        .agg(
            avg("amount").alias("avg_amount"),
            count("*").alias("tx_count"),
            spark_sum("amount").alias("total_amount"),
            approx_count_distinct("counterparty_id").alias("distinct_counterparties"),
            expr("min(event_time)").alias("first_event"),
            expr("max(event_time)").alias("last_event"),
        )
        .withColumn("processed_at", current_timestamp())
        .withColumn(
            "avg_hourly_lifetime",
            col("total_amount")
            / expr("greatest(1.0, (unix_timestamp(last_event) - unix_timestamp(first_event)) / 3600.0)"),
        )
        .withColumn(
            "avg_daily_lifetime",
            col("total_amount")
            / expr("greatest(1.0, (unix_timestamp(last_event) - unix_timestamp(first_event)) / 86400.0)"),
        )
        .withColumn(
            "avg_weekly_lifetime",
            col("total_amount")
            / expr("greatest(1.0, (unix_timestamp(last_event) - unix_timestamp(first_event)) / 604800.0)"),
        )
        .withColumn(
            "avg_monthly_lifetime",
            col("total_amount")
            / expr("greatest(1.0, (unix_timestamp(last_event) - unix_timestamp(first_event)) / 2592000.0)"),
        )
    )
    lifetime.coalesce(1).write.mode("overwrite").parquet(f"{OUTPUT_BASE}/lifetime")


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    spark = batch_df.sparkSession
    parsed = parse_batch(batch_df).cache()
    parsed.write.mode("append").parquet(HISTORY_PATH)

    try:
        history = spark.read.parquet(HISTORY_PATH)
    except Exception:
        history = parsed

    # Keep last micro-batch rows; dashboard applies 10s filter (avoids empty writes when Spark lags)
    recent_out = (
        parsed.select("tx_id", "send_id", "receive_id", "amount", "date", "tx_type", "event_time")
        .orderBy(col("event_time").desc())
        .limit(200)
    )
    recent_out.coalesce(1).write.mode("overwrite").parquet(f"{OUTPUT_BASE}/recent_activity")

    parsed.createOrReplaceTempView("batch_users")
    spark.sql(
        """
        SELECT user_id, max(event_time) AS last_seen
        FROM (
            SELECT send_id AS user_id, event_time FROM batch_users
            UNION ALL
            SELECT receive_id AS user_id, event_time FROM batch_users
        ) u
        GROUP BY user_id
        ORDER BY last_seen DESC
        LIMIT 20
        """
    ).coalesce(1).write.mode("overwrite").parquet(f"{OUTPUT_BASE}/recent_users")

    write_lifetime(history)
    for label, duration, slide in WINDOW_SPECS:
        write_windowed(spark, history, label, duration, slide)

    parsed.unpersist()
    print(f"Batch {batch_id} processed OK", flush=True)


def main() -> None:
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    query = (
        raw.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/processor")
        .trigger(processingTime=TRIGGER)
        .start()
    )

    print("Streaming processor started.", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
