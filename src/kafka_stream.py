from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)
from pyspark.sql.functions import (
    from_json,
    col,
    to_timestamp,
    window,
    sum as spark_sum,
    count,
    avg
)


# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("EcommerceKafkaStreaming")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
    )
    .config("spark.hadoop.io.native.lib.available", "false")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Spark Kafka JSON Streaming Started...")


# --------------------------------------------------
# 2. Define JSON Schema
# --------------------------------------------------

event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("event_time", StringType(), True)
])


# --------------------------------------------------
# 3. Read Data From Kafka
# --------------------------------------------------

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "ecommerce_events")
    .option("startingOffsets", "earliest")
    .load()
)

# --------------------------------------------------
# 4. Bronze Layer - Store Raw Kafka Events
# --------------------------------------------------

bronze_df = df.select(
    col("topic").alias("kafka_topic"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
    col("timestamp").alias("kafka_timestamp"),
    col("value").cast("string").alias("raw_event")
)

bronze_query = (
    bronze_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option(
        "path",
        "data/raw/ecommerce_events"
    )
    .option(
        "checkpointLocation",
        "checkpoints/bronze_ecommerce_events"
    )
    .queryName("bronze_ecommerce_events")
    .start()
)



# --------------------------------------------------
# 4. Convert Kafka Value From Binary to String
# --------------------------------------------------

json_df = df.select(
    col("value").cast("string").alias("json_value")
)


# --------------------------------------------------
# 5. Parse JSON
# --------------------------------------------------

parsed_df = json_df.select(
    from_json(col("json_value"), event_schema).alias("data")
)


# --------------------------------------------------
# 6. Extract JSON Fields
# --------------------------------------------------

events_df = parsed_df.select(
    "data.event_id",
    "data.event_type",
    "data.order_id",
    "data.customer_id",
    "data.product_id",
    "data.quantity",
    "data.amount",
    "data.city",
    to_timestamp("data.event_time").alias("event_time")
)


# --------------------------------------------------
# 7. Data Quality Validation
# --------------------------------------------------

valid_events_df = events_df.filter(
    col("event_id").isNotNull()
    & col("customer_id").isNotNull()
    & col("order_id").isNotNull()
    & col("city").isNotNull()
    & col("event_time").isNotNull()
    & (col("amount") > 0)
    & (col("quantity") > 0)
)

invalid_events_df = events_df.filter(
    col("event_id").isNull()
    | col("customer_id").isNull()
    | col("order_id").isNull()
    | col("city").isNull()
    | col("event_time").isNull()
    | (col("amount") <= 0)
    | (col("quantity") <= 0)
)




# --------------------------------------------------
# Silver Layer - Clean Ecommerce Events
# --------------------------------------------------

silver_query = (
    valid_events_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option(
        "path",
        "data/processed/ecommerce_events"
    )
    .option(
        "checkpointLocation",
        "checkpoints/silver_ecommerce_events"
    )
    .queryName("silver_ecommerce_events")
    .start()
)

# --------------------------------------------------
# 8. Keep Only Successful Payments
# --------------------------------------------------

payment_df = valid_events_df.filter(
    col("event_type") == "payment_completed"
)

# --------------------------------------------------
# Invalid Events - Data Quality Monitoring
# --------------------------------------------------

invalid_query = (
    invalid_events_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", 20)
    .queryName("invalid_events")
    .start()
)


# --------------------------------------------------
# 8. Add Watermark
# --------------------------------------------------

watermarked_df = (
    payment_df
    .withWatermark("event_time", "10 minutes")
)


# --------------------------------------------------
# 9. Create 1-Minute Window Aggregation
# --------------------------------------------------

payment_summary = (
    watermarked_df
    .groupBy(
        window("event_time", "1 minute"),
        col("city")
    )
    .agg(
        spark_sum("amount").alias("total_payment_amount"),
        count("*").alias("payment_count"),
        avg("amount").alias("average_payment_amount")
    )
)


# --------------------------------------------------
# 10. Write Aggregated Data To Console
# --------------------------------------------------

query = (
    payment_summary.writeStream
    .format("parquet")
    .outputMode("append")
    .option(
        "path",
        "data/processed/payment_summary"
    )
    .option(
        "checkpointLocation",
        "checkpoints/payment_summary"
    )
    .queryName("payment_summary")
    .start()
)


spark.streams.awaitAnyTermination()