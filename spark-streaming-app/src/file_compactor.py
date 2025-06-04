from pyspark.sql import SparkSession
import os
from datetime import datetime, timedelta

def create_spark_session():
    return SparkSession.builder \
        .appName("FileCompactor") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

def compact_parquet_files(spark, input_path, output_path, tmp_path):
    """
    Lit tous les fichiers Parquet d'un dossier et les réécrit en un nombre réduit de fichiers
    """
    # Lire tous les fichiers Parquet du dossier
    df = spark.read.parquet(input_path)
    
    # Repartitionner en un nombre raisonnable de fichiers (ajuster selon vos besoins)
    num_partitions = 4
    
    # Écrire dans un dossier temporaire
    df.repartition(num_partitions).write.mode("overwrite").parquet(tmp_path)
    
    # Supprimer l'ancien dossier et renommer le nouveau
    spark._jvm.org.apache.hadoop.fs.FileUtil.delete(
        spark._jvm.java.io.File(output_path), True)
    os.rename(tmp_path, output_path)

def compact_json(input_path, output_path):
    spark = SparkSession.builder.appName("FileCompactor").getOrCreate()
    df = spark.read.option("mergeSchema", "true").json(input_path)
    df.coalesce(1).write.mode("overwrite").json(output_path)
    spark.stop()

def process_metrics_folders(spark, base_paths):
    """
    Traite tous les dossiers de métriques
    """
    for base_path in base_paths:
        if not os.path.exists(base_path):
            print(f"Le dossier {base_path} n'existe pas encore")
            continue
            
        for metric_folder in os.listdir(base_path):
            input_path = os.path.join(base_path, metric_folder)
            if not os.path.isdir(input_path):
                continue
                
            tmp_path = f"{input_path}_tmp_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            try:
                print(f"Compactage des fichiers Parquet dans {input_path}")
                compact_parquet_files(spark, input_path, input_path, tmp_path)
                print(f"Compactage terminé pour {input_path}")
            except Exception as e:
                print(f"Erreur lors du compactage de {input_path}: {str(e)}")
            
            try:
                output_json_path = os.path.join(input_path, "compacted")
                print(f"Compactage des fichiers JSON dans {input_path} vers {output_json_path}")
                compact_json(input_path, output_json_path)
                print(f"Compactage JSON terminé pour {input_path}")
            except Exception as e:
                print(f"Erreur lors du compactage JSON de {input_path}: {str(e)}")

def main():    # Chemins des dossiers à compacter
    base_paths = [
        "C:/Users/potet/Documents/DATALAKEAPI/TPSPARK/data/streaming",
        "C:/Users/potet/Documents/DATALAKEAPI/TPSPARK/data/batch"
    ]
    
    spark = create_spark_session()
    
    try:
        process_metrics_folders(spark, base_paths)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
