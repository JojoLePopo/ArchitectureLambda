# Spark Streaming Application

Ce projet est une application Spark Streaming qui lit des logs depuis un topic Kafka, les traite en temps réel, et écrit les résultats agrégés sur le système de fichiers local (formats CSV, Parquet ou Delta selon la configuration).

---

## Structure du projet

```
spark-streaming-app
├── src
│   ├── streaming_metrics.py   # Traitement streaming principal (CSV/JSON)
│   ├── delta_metrics.py       # Traitement streaming avec Delta Lake
│   └── utils
│       └── __init__.py        # Fonctions utilitaires (optionnel)
├── requirements.txt           # Dépendances Python
├── Log_Simulate.py            # Générateur de logs pour Kafka
└── README.md                  # Documentation du projet
```


---

## Arborescence du dossier `data`

Après exécution, la structure du dossier `data` ressemblera à ceci :

```
data
├── raw/                        # Données brutes issues de Kafka (CSV)
│   └── ...                     # Fichiers CSV bruts
├── metrics/                    # Résultats agrégés (CSV/Parquet)
│   ├── connections_by_ip/      # Agrégations par IP
│   ├── connections_by_agent/   # Agrégations par user agent
│   └── connections_by_day/     # Agrégations par jour
├── delta/                      # Résultats agrégés au format Delta Lake (si utilisé)
│   ├── ip_counts/
│   ├── agent_counts/
│   └── daily_counts/
└── ...                         # Autres dossiers/checkpoints générés par Spark
```
---

## Prérequis

- Python 3.8+
- Apache Spark 3.5.x
- Kafka en local (topic `transaction_log` créé)
- Java 8 ou 11

---

## Installation

1. **Cloner le dépôt :**
   ```bash
   git clone <repository-url>
   cd spark-streaming-app
   ```

2. **Installer les dépendances Python :**
   ```bash
   pip install -r requirements.txt
   ```

3. **Lancer Kafka et créer le topic :**
   ```bash
   # Exemple avec Kafka en local
   kafka-topics --create --topic transaction_log --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```

---

## Utilisation

### 1. **Simuler des logs dans Kafka**
Lancer le simulateur pour alimenter le topic :
```bash
python Log_Simulate.py
```

### 2. **Démarrer le traitement Spark Streaming**
- **Traitement classique (CSV/JSON) :**
  ```bash
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 src/streaming_metrics.py
  ```
- **Traitement Delta Lake (recommandé pour l'outputMode 'complete') :**
  ```bash
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-core_2.12:2.4.0 src/delta_metrics.py
  ```

### 3. **Résultats**
- Les fichiers agrégés sont écrits dans le dossier `data/metrics` ou `data/delta` selon le script utilisé.
- Les données brutes sont sauvegardées dans `data/raw`.

---

## Fonctionnalités

- Lecture temps réel depuis Kafka
- Parsing de logs CSV (timestamp, ip, user_agent)
- Agrégations par IP, user agent, et par jour
- Sauvegarde des résultats en CSV, Parquet ou Delta Lake
- Gestion des fenêtres temporelles et watermark pour les agrégations streaming

---

## Auteurs

- Joseph Destat Guillot
- Thomas Coutarel

---

## Licence

Ce projet est sous licence MIT.