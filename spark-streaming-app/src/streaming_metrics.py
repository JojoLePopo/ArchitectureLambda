from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, to_timestamp, window, split
from pyspark.sql.types import StructType, StructField, StringType
import os

base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
parent_path = os.path.dirname(base_path)

metrics_paths = {
    "raw_data": os.path.join(parent_path, "data/raw"),
    "ip_counts": os.path.join(parent_path, "data/metrics/connections_by_ip"),
    "agent_counts": os.path.join(parent_path, "data/metrics/connections_by_agent"),
    "daily_counts": os.path.join(parent_path, "data/metrics/connections_by_day")
}

print(f"Utilisation des dossiers de métriques :")
for k, v in metrics_paths.items():
    print(f"- {k}: {v}")
    os.makedirs(v, exist_ok=True)

def create_spark_session():
    return SparkSession.builder \
        .appName("StreamingMetrics") \
        .getOrCreate()

def create_streaming_dataframe(spark):
    df_raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "transaction_log") \
        .option("startingOffsets", "earliest") \
        .load()

    df = df_raw.selectExpr("CAST(value AS STRING) as csv_str") \
        .withColumn("fields", split(col("csv_str"), ",(?=(?:[^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)")) \
        .select(
            col("fields").getItem(0).alias("timestamp"),
            col("fields").getItem(1).alias("ip"),
            col("fields").getItem(2).alias("browser")
        ) \
        .withColumn("event_time", to_timestamp(col("timestamp")))
    return df

def setup_streaming_queries(df):
    queries = []
    # 1. Sauvegarde des données brutes
    raw_query = df.writeStream \
        .outputMode("append") \
        .format("csv") \
        .option("path", metrics_paths["raw_data"]) \
        .option("checkpointLocation", f"{metrics_paths['raw_data']}_checkpoint") \
        .option("header", "true") \
        .start()
    queries.append(raw_query)
    # 2. Connexions par IP (fenêtre de 1 minute)
    ip_df = df.withWatermark("event_time", "1 minute") \
        .groupBy(window(col("event_time"), "1 minute"), col("ip")) \
        .count() \
        .select(
            col("window.start").alias("window_start"),
            col("ip"),
            col("count").alias("connection_count")
        )
    ip_query = ip_df.writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", metrics_paths["ip_counts"]) \
        .option("checkpointLocation", f"{metrics_paths['ip_counts']}_checkpoint") \
        .start()
    queries.append(ip_query)
    # 3. Connexions par agent (fenêtre de 1 minute)
    agent_df = df.withWatermark("event_time", "1 minute") \
        .groupBy(window(col("event_time"), "1 minute"), col("browser")) \
        .count() \
        .select(
            col("window.start").alias("window_start"),
            col("browser"),
            col("count").alias("connection_count")
        )
    agent_query = agent_df.writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", metrics_paths["agent_counts"]) \
        .option("checkpointLocation", f"{metrics_paths['agent_counts']}_checkpoint") \
        .start()
    queries.append(agent_query)
    # 4. Connexions par jour (déjà fenêtré)
    daily_df = df.withWatermark("event_time", "1 day") \
        .groupBy(window("event_time", "1 day")) \
        .count() \
        .select(col("window.start").alias("date"), col("count").alias("connection_count"))
    daily_query = daily_df.writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", metrics_paths["daily_counts"]) \
        .option("checkpointLocation", f"{metrics_paths['daily_counts']}_checkpoint") \
        .start()
    queries.append(daily_query)
    return queries

def main():
    spark = create_spark_session()
    queries = []
    
    try:
        df = create_streaming_dataframe(spark)
        queries = setup_streaming_queries(df)
        
        for query in queries:
            query.awaitTermination()
            
    except KeyboardInterrupt:
        print("Arrêt des queries streaming...")
        for query in queries:
            query.stop()
    except Exception as e:
        print(f"Erreur lors du traitement streaming: {str(e)}")
        for query in queries:
            query.stop()

if __name__ == "__main__":
    main()
