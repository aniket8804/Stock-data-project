from kafka import KafkaConsumer
import snowflake.connector
import json

# Snowflake connection
conn = snowflake.connector.connect(
    user='YOUR_USERNAME',
    password='YOUR_PASSWORD',
    account='VTVWDRP-AZ20971',
    warehouse='COMPUTE_WH',
    database='STOCK_DB',
    schema='STOCK_SCHEMA'
)

cursor = conn.cursor()

# Kafka consumer
consumer = KafkaConsumer(
    'stock-topic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Consumer started...")

for message in consumer:
    data = message.value

    symbol = data["symbol"]
    price = data["price"]
    timestamp = data["timestamp"]

    insert_query = f"""
    INSERT INTO STOCK_DATA (SYMBOL, PRICE, TIMESTAMP)
    VALUES ('{symbol}', {price}, CURRENT_TIMESTAMP)
    """

    cursor.execute(insert_query)

    print(f"Inserted into Snowflake: {symbol} {price}")