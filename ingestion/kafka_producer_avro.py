import json
import time
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import websocket

# Replace with your real API key
FINNHUB_API_KEY = "d05ghqpr01qoigrug80gd05ghqpr01qoigrug810"
KAFKA_TOPIC = "stock_stream_avro"
SCHEMA_REGISTRY_URL = 'http://localhost:8081'

# List of stock symbols to track
SYMBOLS = ["AAPL", "TSLA", "MSFT", "AMZN", "GOOGL"]

# Avro Schema (value)
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

# No need for key schema now
value_schema = avro.loads(value_schema_str)

producer_config = {
    'bootstrap.servers': 'localhost:9092',
    'schema.registry.url': SCHEMA_REGISTRY_URL
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

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed")

def on_open(ws):
    for symbol in SYMBOLS:
        ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))

if __name__ == "__main__":
    socket = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"
    ws = websocket.WebSocketApp(socket,
                                 on_open=on_open,
                                 on_message=on_message,
                                 on_error=on_error,
                                 on_close=on_close)
    ws.run_forever()

