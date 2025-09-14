import csv
import json
from kafka import KafkaProducer
from datetime import datetime
import time

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# CSV file containing transactions
CSV_FILE = 'atm_transactions1.csv'

with open(CSV_FILE, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        # Convert timestamp string to ISO format if needed
        row['timestamp'] = datetime.fromisoformat(row['timestamp']).isoformat()
        # Convert amount to float
        row['amount'] = float(row['amount'])
        producer.send('atm-transaction-topic', row)
        print(f"Produced: {row}")
        time.sleep(0.05)  # simulate streaming

producer.flush()
print("All records from CSV streamed to Kafka.")
