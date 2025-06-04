from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import os
from delta import configure_spark_with_delta_pip

def create_spark_session():
    builder = SparkSession.builder.appName("DeltaMetrics") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    return configure_spark_with_delta_pip(builder).getOrCreate()

# Chemins de sortie pour les différentes métriques
output_base = "C:/Users/potet/Documents/DATALAKEAPI/TPSPARK/data/delta"
metrics_paths = {
    "ip_counts": f"{output_base}/ip_counts",
    "agent_counts": f"{output_base}/agent_counts",
    "daily_counts": f"{output_base}/daily_counts"
}

def setup_streaming_queries(df):
    # Créer les dossiers de sortie
    for path in metrics_paths.values():
        os.makedirs(path, exist_ok=True)
        checkpoint_path = f"{path}_checkpoint"
        os.makedirs(checkpoint_path, exist_ok=True)

    # 1. Nombre de connexions par IP (fenêtre de 5 minutes)
    ip_counts = df.groupBy(
        window(col("event_time"), "5 minutes"),
        "ip"
    ).agg(count("*").alias("connection_count"))

    # 2. Nombre de connexions par agent (fenêtre de 5 minutes)
    agent_counts = df.groupBy(
        window(col("event_time"), "5 minutes"),
        "browser"
    ).agg(count("*").alias("connection_count"))

    # 3. Nombre de connexions par jour
    daily_counts = df.groupBy(
        window(col("event_time"), "1 day")
    ).agg(count("*").alias("connection_count"))

    # Démarrer les queries streaming avec Delta Lake
    queries = []
    
    # Query pour les comptages par IP
    queries.append(ip_counts.writeStream
        .format("delta")
        .outputMode("complete")
        .option("checkpointLocation", f"{metrics_paths['ip_counts']}_checkpoint")
        .start(metrics_paths["ip_counts"]))

    # Query pour les comptages par agent
    queries.append(agent_counts.writeStream
        .format("delta")
        .outputMode("complete")
        .option("checkpointLocation", f"{metrics_paths['agent_counts']}_checkpoint")
        .start(metrics_paths["agent_counts"]))

    # Query pour les comptages quotidiens
    queries.append(daily_counts.writeStream
        .format("delta")
        .outputMode("complete")
        .option("checkpointLocation", f"{metrics_paths['daily_counts']}_checkpoint")
        .start(metrics_paths["daily_counts"]))

    return queries

def create_streaming_dataframe(spark):
    # Schema pour les données d'entrée
    schema = StructType([
        StructField("timestamp", StringType()),
        StructField("device_info", StructType([
            StructField("ip_address", StringType()),
            StructField("browser", StringType())
        ]))
    ])

    # Lecture du stream Kafka
    df_raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "transaction_log") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parsing du JSON
    df = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", to_timestamp("timestamp")) \
        .withColumn("ip", col("device_info.ip_address")) \
        .withColumn("browser", col("device_info.browser"))

    return df

def main():
    spark = create_spark_session()
    
    try:
        df = create_streaming_dataframe(spark)
        queries = setup_streaming_queries(df)
        
        # Attendre la terminaison de toutes les queries
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

# REMARQUE IMPORTANTE :
# Ce script fonctionne car le format 'delta' supporte le mode 'complete'.
# Si vous souhaitez utiliser un autre format (json/csv), il faut changer le mode en 'append' ou 'update'.

if __name__ == "__main__":
    main()
