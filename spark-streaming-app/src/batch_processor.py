from pyspark.sql import SparkSession
from pyspark.sql.functions import count, date_format, to_date, col, to_timestamp, window
import os
import time

# Chemins absolus pour les métriques
base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
parent_path = os.path.dirname(base_path)

metrics_paths = {
    "raw_data": os.path.join(parent_path, "data/raw"),
    "ip_counts_batch": os.path.join(parent_path, "data/metrics/connections_by_ip/batch"),
    "agent_counts_batch": os.path.join(parent_path, "data/metrics/connections_by_agent/batch"),
    "daily_counts_batch": os.path.join(parent_path, "data/metrics/connections_by_day/batch")
}

# Créer les dossiers s'ils n'existent pas
for path in metrics_paths.values():
    os.makedirs(path, exist_ok=True)

def create_spark_session():
    return SparkSession.builder \
        .appName("BatchProcessor") \
        .getOrCreate()

def process_batch_data(spark, input_path):
    print(f"Lecture des données depuis {input_path}")
    
    # Lecture de toutes les données CSV brutes
    df = spark.read \
        .option("header", "true") \
        .csv(input_path)
    
    print(f"Nombre total de lignes lues : {df.count()}")
    
    # Conversion des timestamps
    df = df.withColumn("event_time", to_timestamp(col("timestamp"))) \
        .withColumn("date", to_date("event_time"))
    
    # 1. Nombre de connexions par IP (total)
    ip_counts = df.groupBy("ip") \
        .agg(count("*").alias("connection_count")) \
        .orderBy(col("connection_count").desc())
    
    # 2. Nombre de connexions par agent (total)
    agent_counts = df.groupBy("browser") \
        .agg(count("*").alias("connection_count")) \
        .orderBy(col("connection_count").desc())
    
    # 3. Nombre de connexions par jour
    daily_counts = df.groupBy("date") \
        .agg(count("*").alias("connection_count")) \
        .orderBy("date")
    
    return ip_counts, agent_counts, daily_counts

def save_metrics(ip_counts, agent_counts, daily_counts):
    # Sauvegarder les métriques en CSV avec horodatage
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    
    ip_counts.write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{metrics_paths['ip_counts_batch']}_{timestamp}")
    
    agent_counts.write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{metrics_paths['agent_counts_batch']}_{timestamp}")
    
    daily_counts.write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{metrics_paths['daily_counts_batch']}_{timestamp}")
    
    print(f"Métriques batch sauvegardées avec horodatage {timestamp}")

def main():
    spark = create_spark_session()
    
    try:
        while True:
            try:
                print("\nDémarrage du traitement batch...")
                ip_counts, agent_counts, daily_counts = process_batch_data(spark, metrics_paths["raw_data"])
                save_metrics(ip_counts, agent_counts, daily_counts)
                print("Traitement batch terminé. Attente de 5 minutes avant le prochain traitement...")
                time.sleep(300)  # 5 minutes entre chaque traitement batch
            except Exception as e:
                print(f"Erreur pendant le traitement batch: {str(e)}")
                print("Nouvelle tentative dans 1 minute...")
                time.sleep(60)
            
    except KeyboardInterrupt:
        print("\nArrêt du traitement batch...")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
