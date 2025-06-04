from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import to_timestamp
import os

output_dir = "C:/Users/potet/Documents/DATALAKEAPI/TPSPARK/data/streaming/output"
checkpoint_dir = "C:/Users/potet/Documents/DATALAKEAPI/TPSPARK/data/streaming/checkpoint"
os.makedirs(output_dir, exist_ok=True)
os.makedirs(checkpoint_dir, exist_ok=True)

spark = SparkSession.builder \
    .appName("SparkStreamingKafka") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

schema = StructType([
    StructField("timestamp", StringType()),
    StructField("device_info", StructType([
        StructField("ip_address", StringType()),
        StructField("browser", StringType())
    ]))
])

df_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transaction_log") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

df = df_json \
    .withColumn("event_time", to_timestamp("timestamp")) \
    .withColumn("ip", col("device_info.ip_address"))

# Aggregate the data by one-hour windows and IP address
agg = df.groupBy(
    window(col("event_time"), "1 hour"),
    col("ip")
).count()

# Write the aggregated results to the local file system in Parquet format
query = agg.writeStream \
    .outputMode("complete") \
    .format("parquet") \
    .option("path", output_dir) \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

# Add proper error handling
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping the streaming query...")
    query.stop()
except Exception as e:
    print(f"An error occurred: {str(e)}")
    query.stop()