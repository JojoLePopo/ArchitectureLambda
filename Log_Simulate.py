from kafka import KafkaProducer
import time
import random
import json
from datetime import datetime

topic_name = 'transaction_log'

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092']
)

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X)",
    "Mozilla/5.0 (Linux; Android 11; SM-G991B)"
]

def generate_ip():
    """Générer une adresse IP aléatoire"""
    return f"192.168.{random.randint(0, 255)}.{random.randint(1, 254)}"

def generate_log():
    """Générer un log au format CSV (timestamp,ip,user_agent)"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ip = generate_ip()
    user_agent = random.choice(user_agents)
    return f'{timestamp},{ip},"{user_agent}"'

if __name__ == '__main__':
    print(f"Démarrage de la génération des logs vers le topic {topic_name}...")
    while True:
        try:
            log_entry = generate_log()
            producer.send(topic_name, log_entry.encode('utf-8'))
            print(f"Log envoyé: {log_entry}")
            time.sleep(random.uniform(1, 3)) 
            
        except Exception as e:
            print(f"Erreur: {e}")
            continue
