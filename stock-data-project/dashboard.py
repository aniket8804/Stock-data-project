import streamlit as st
import snowflake.connector
import pandas as pd

st.set_page_config(page_title="Real-Time Stock Dashboard", layout="wide")

st.title("📈 Real-Time Stock Market Dashboard")

# Snowflake connection
conn = snowflake.connector.connect(
    user='YOUR_USERNAME',
    password='YOUR_PASSWORD',
    account='VTVWDRP-AZ20971',
    warehouse='COMPUTE_WH',
    database='STOCK_DB',
    schema='STOCK_SCHEMA'
)

# Load data
@st.cache_data(ttl=30)
def load_data():
    query = """
        SELECT SYMBOL, PRICE, TIMESTAMP
        FROM CLEAN_STOCK
        ORDER BY TIMESTAMP DESC
    """
    df = pd.read_sql(query, conn)
    return df

df = load_data()

if df.empty:
    st.warning("No stock data available.")
else:

    # Latest price table
    st.subheader("📊 Latest Stock Prices")
    latest_prices = df.groupby("SYMBOL").first().reset_index()
    st.dataframe(latest_prices)

    # Stock selector
    st.subheader("📉 Stock Trend Analysis")
    selected_stock = st.selectbox("Choose Stock", df["SYMBOL"].unique())

    filtered_df = df[df["SYMBOL"] == selected_stock].sort_values("TIMESTAMP")

    # Line chart
    st.line_chart(filtered_df.set_index("TIMESTAMP")["PRICE"])

    # Metrics
    st.subheader("📌 Key Metrics")
    col1, col2 = st.columns(2)

    latest_price = filtered_df.iloc[-1]["PRICE"]
    prev_price = filtered_df.iloc[-2]["PRICE"] if len(filtered_df) > 1 else latest_price

    change = latest_price - prev_price
    change_percent = (change / prev_price) * 100 if prev_price != 0 else 0

    col1.metric("Latest Price", f"${latest_price:.2f}")
    col2.metric("Price Change", f"{change:.2f} ({change_percent:.2f}%)")