# 🚀 Fraud Detection Streaming Analysis  

A real-time **fraud detection system** that simulates ATM transactions, streams them via **Apache Kafka**, applies fraud detection rules in a **Python consumer**, and stores results in **MongoDB** for analysis and dashboards.  

---

## 📌 Features  
- Generated **ATM transaction data** using Python.  
- Streamed transactions from CSV to Kafka in real-time.  
- Fraud detection rules:  
  - High-value transactions (₹20,000+).  
  - Location mismatch (outside customer’s usual area).  
  - Multiple transactions in short intervals.  
- Stored data in MongoDB:  
  - `atm_transactions` → all records.  
  - `fraud_alerts` → flagged transactions.  

---

## 🏗️ System Flow  
**Data Generator (Python CSV) → Producer (CSV → Kafka) → Kafka Topic → Consumer (fraud detection) → MongoDB (transactions + alerts) → Dashboard**  
