import json
import time
import websocket
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

FINNHUB_API_KEY = "d08msppr01qju5m7gb0gd08msppr01qju5m7gb10"
KAFKA_TOPIC = "stock_stream_avro"
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
SYMBOLS = ["AAPL", "TSLA", "MSFT", "AMZN", "GOOGL"]

value_schema_str = """
{
  "namespace": "stock.price",
  "type": "record",
  "name": "StockTrade",
  "fields": [
    {"name": "symbol", "type": "string"},
    {"name": "price", "type": "double"},
    {"name": "volume", "type": "double"},
    {"name": "timestamp", "type": "long"}
  ]
}
"""

value_schema = avro.loads(value_schema_str)

producer_config = {
    'bootstrap.servers': 'localhost:9092',
    'schema.registry.url': 'http://localhost:8081'
}

producer = AvroProducer(producer_config, default_value_schema=value_schema)

def on_message(ws, message):
    data = json.loads(message)
    if data['type'] == 'trade':
        for trade in data['data']:
            record = {
                "symbol": trade['s'],
                "price": trade['p'],
                "volume": trade['v'],
                "timestamp": trade['t']
            }
            print(f"Sending: {record}")
            producer.produce(topic=KAFKA_TOPIC, value=record)
            producer.flush()

def on_error(ws, error):
    print("WebSocket error:", error)
    print("Attempting to reconnect in 5 seconds...")
    time.sleep(5)
    start_socket()

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed")
    print("Attempting to reconnect in 5 seconds...")
    time.sleep(5)
    start_socket()

def on_open(ws):
    for symbol in SYMBOLS:
        ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))

def start_socket():
    socket_url = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"
    ws = websocket.WebSocketApp(socket_url,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.run_forever()

if __name__ == "__main__":
    # Test message to validate AvroProducer + Schema Registry + Kafka
    test_record = {
        "symbol": "TEST",
        "price": 123.45,
        "volume": 1000,
        "timestamp": int(time.time() * 1000)
    }
    print("Sending test record...")
    producer.produce(topic=KAFKA_TOPIC, value=test_record)
    producer.flush()

    start_socket()

