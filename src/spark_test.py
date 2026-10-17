from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("EcommerceKafkaStreaming")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Spark Kafka Streaming Started...")

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "ecommerce_events")
    .option("startingOffsets", "earliest")
    .load()
)

events_df = df.selectExpr(
    "CAST(key AS STRING) AS key",
    "CAST(value AS STRING) AS value"
)

query = (
    events_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()