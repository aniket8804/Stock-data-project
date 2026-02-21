{{ config(
    materialized='incremental',
    unique_key='symbol_timestamp'
) }}

SELECT
    SYMBOL,
    PRICE,
    TIMESTAMP,
    SYMBOL || '_' || TO_VARCHAR(TIMESTAMP) AS symbol_timestamp,
    LAG(PRICE) OVER (PARTITION BY SYMBOL ORDER BY TIMESTAMP) AS PREVIOUS_PRICE,
    PRICE - LAG(PRICE) OVER (PARTITION BY SYMBOL ORDER BY TIMESTAMP) AS PRICE_CHANGE

FROM STOCK_DB.STOCK_SCHEMA.STOCK_DATA

{% if is_incremental() %}

WHERE (SYMBOL || '_' || TO_VARCHAR(TIMESTAMP)) NOT IN (
    SELECT symbol_timestamp FROM {{ this }}
)

{% endif %}