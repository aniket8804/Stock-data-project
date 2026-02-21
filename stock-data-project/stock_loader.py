import snowflake.connector
import requests
from datetime import datetime
import time

# Snowflake connection
conn = snowflake.connector.connect(
    user='ANIKET880',
    password='Aniket@123456789',
    account='VTVWDRP-AZ20971',
    warehouse='COMPUTE_WH',
    database='STOCK_DB',
    schema='STOCK_SCHEMA'
)

cursor = conn.cursor()

API_KEY = "d6c5sk9r01qsiik0s740d6c5sk9r01qsiik0s74g"

symbols = ["AAPL", "MSFT", "TSLA", "AMZN"]

while True:
    for symbol in symbols:
        url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={API_KEY}"
        response = requests.get(url)
        data = response.json()

        price = data["c"]
        timestamp = datetime.now()

        insert_query = f"""
        INSERT INTO STOCK_DATA (SYMBOL, PRICE, TIMESTAMP)
        VALUES ('{symbol}', {price}, '{timestamp}')
        """

        cursor.execute(insert_query)

        print(f"{symbol} inserted")

    time.sleep(6)  # runs every 6 seconds