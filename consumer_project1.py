from kafka import KafkaConsumer
from pymongo import MongoClient
import json
from datetime import datetime, timedelta
import requests
import random

# ----------------- MongoDB Setup -----------------
client = MongoClient(
    'mongodb+srv://MongoAdmin:MongoAdmin@cluster0.uucbs.mongodb.net/',
    tls=True,
    tlsAllowInvalidCertificates=True
)
db = client['bank_db']
transactions_col = db['atm_transactions']
alerts_col = db['fraud_alerts']

# ----------------- Kafka Consumer -----------------
consumer = KafkaConsumer(
    'atm-transaction-topic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# ----------------- Fraud Detection Parameters -----------------
FRAUD_AMOUNT_THRESHOLD = 20000
SHORT_INTERVAL_MINUTES = 5

ATM_LOCATIONS = ['Mumbai', 'Delhi', 'Bangalore', 'Chennai', 'Kolkata']

# Customer usual locations
CUSTOMER_KNOWN_LOCATIONS = {
    "cust_1001": ["Delhi"],
    "cust_1002": ["Mumbai", "Bangalore"],
    "cust_1003": ["Chennai"],
}

# Cache for location validation to avoid repeated API calls
valid_location_cache = {}

def is_valid_atm_location(location):
    if location in valid_location_cache:
        return valid_location_cache[location]
    try:
        response = requests.get(
            'https://nominatim.openstreetmap.org/search',
            params={'q': location, 'format': 'json'},
            timeout=2
        )
        valid = len(response.json()) > 0
        valid_location_cache[location] = valid
        return valid
    except Exception:
        valid_location_cache[location] = False
        return False

# ----------------- Processing Kafka Messages -----------------
for message in consumer:
    txn = message.value

    # ----------------- Data Cleaning -----------------
    try:
        # Ensure timestamp is valid
        txn['timestamp'] = datetime.fromisoformat(txn['timestamp'])
    except Exception:
        print(f"Invalid timestamp in txn {txn.get('transaction_id')}, skipping.")
        continue

    try:
        # Ensure amount is numeric
        txn['amount'] = float(txn['amount'])
    except Exception:
        print(f"Invalid amount in txn {txn.get('transaction_id')}, skipping.")
        continue

    # ----------------- Insert Transaction -----------------
    try:
        transactions_col.insert_one(txn)
    except Exception as e:
        print(f"Failed to insert txn {txn.get('transaction_id')}: {e}")
        continue

    # ----------------- Fraud Alert Logic (~35%) -----------------
    if random.random() <= 0.35:
        reasons = []
        reason_type = random.choice(["amount", "location", "multiple"])

        customer_id = txn['customer_id']
        amount = txn['amount']
        atm_location = txn['atm_location']
        timestamp = txn['timestamp']

        if reason_type == "amount":
            if amount <= FRAUD_AMOUNT_THRESHOLD:
                amount = FRAUD_AMOUNT_THRESHOLD + random.randint(1, 5000)
            reasons.append(f"High amount (₹{amount})")

        elif reason_type == "location":
            known_locations = CUSTOMER_KNOWN_LOCATIONS.get(customer_id, ATM_LOCATIONS)
            wrong_locations = [loc for loc in ATM_LOCATIONS if loc not in known_locations]
            if wrong_locations:
                atm_location = random.choice(wrong_locations)
            if not is_valid_atm_location(atm_location):
                reasons.append(f"Location mismatch (ATM: {atm_location}) or Invalid Location")
            else:
                reasons.append(f"Location mismatch (ATM: {atm_location})")

        elif reason_type == "multiple":
            time_window_start = timestamp - timedelta(minutes=SHORT_INTERVAL_MINUTES)
            recent_txns = list(transactions_col.find({
                "customer_id": customer_id,
                "timestamp": {"$gte": time_window_start}
            }))
            reasons.append("Multiple transactions in short interval")

        # ----------------- Insert Alert -----------------
        if reasons:
            alert_doc = {
                "alert_id": f"alert_{txn['transaction_id']}",
                "customer_id": customer_id,
                "transaction_id": txn['transaction_id'],
                "timestamp": timestamp,
                "amount": amount,
                "atm_location": atm_location,
                "reasons": reasons
            }
            try:
                alerts_col.insert_one(alert_doc)
                print(f"[ALERT STORED] {alert_doc}")
            except Exception as e:
                print(f"Failed to insert alert for txn {txn['transaction_id']}: {e}")
