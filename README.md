# Real-Time Stock Price Pipeline 🚀📈

A full-stack real-time stock streaming and analytics pipeline built using Apache Kafka, Apache Spark, AWS S3, Prometheus, Streamlit, Airflow, and more.

## 🌐 Overview

This project simulates a real-world stock monitoring system. It:
- Streams live stock prices from WebSocket APIs
- Publishes them to Apache Kafka
- Logs raw and transformed data to AWS S3
- Performs real-time analytics with Apache Spark
- Generates alerts on anomalies
- Visualizes trends via Streamlit
- Uses Airflow for orchestration and Prometheus for monitoring

## 📦 Tech Stack

| Stage                  | Tool/Service              |
|------------------------|---------------------------|
| Ingestion              | Apache Kafka, WebSockets  |
| Storage                | AWS S3, PostgreSQL        |
| Processing             | Apache Spark              |
| Analysis               | Spark MLlib               |
| Orchestration          | Apache Airflow            |
| Visualization          | Streamlit                 |
| Monitoring             | Prometheus + Grafana      |
| Metadata Management    | Apache Atlas (planned)    |

## 📁 Current Modules

| Module       | Description                          |
|--------------|--------------------------------------|
| `ingestion/` | Kafka producer for stock price feed  |
| `data/`      | Sample output from Kafka stream      |
| `logs/`      | Execution logs and error tracking    |

## 🔧 Setup

```bash
# Clone this repo
git clone https://github.com/Nerdboss-stm/Real-Time-Stock-Price-Pipeline.git
cd Real-Time-Stock-Price-Pipeline

# Install dependencies
pip install -r requirements.txt

# Start Kafka before running producer
python ingestion/kafka_producer.py

> ⚙️ Spark Version: 3.5.5  
> 🧩 Kafka Integration: spark-sql-kafka-0-10_2.12:3.5.0

