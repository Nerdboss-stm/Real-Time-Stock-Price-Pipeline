
## 🐍 `kafka_producer.py`

import json
import time
import websocket
from kafka import KafkaProducer
import logging
logging.basicConfig(
    filename='../logs/kafka_producer.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Replace with your actual key from finnhub.io
FINNHUB_API_KEY = "d05ghqpr01qoigrug80gd05ghqpr01qoigrug810"
KAFKA_TOPIC = "stock_stream"
SYMBOLS = ["BINANCE:BTCUSDT", "BINANCE:ETHUSDT"]

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def on_message(ws, message):
    data = json.loads(message)
    print("Sending:", data)
    logging.info(f"Published: {data}")
    producer.send(KAFKA_TOPIC, value=data)

def on_error(ws, error):
    print("WebSocket error:", error)
    logging.error(f"Error: {error}")

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

