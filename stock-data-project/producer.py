from kafka import KafkaProducer
import requests
import json
import time

API_KEY = "YOUR_API_KEY"

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

symbols = ["AAPL", "MSFT", "TSLA", "AMZN"]

print("Producer started...")

while True:
    for symbol in symbols:
        try:
            url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={API_KEY}"
            response = requests.get(url, timeout=5)

            # Skip if API failed
            if response.status_code != 200:
                print(f"API error {response.status_code} for {symbol}:", response.text)
                continue

            data = response.json()

            # Skip if empty response
            if not data or "c" not in data:
                print(f"No data for {symbol}")
                continue

            message = {
                "symbol": symbol,
                "price": data.get("c"),
                "timestamp": time.time()
            }

            producer.send("stock-topic", value=message)
            print(f"Sent: {message}")

        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
            continue

    time.sleep(6)