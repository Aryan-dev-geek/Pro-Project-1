from datetime import datetime
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# Initialize Spark Session with Unity Catalog support
spark = SparkSession.builder.appName("CoinGeckoBronzeUCAppendIngestion").getOrCreate()

# Fetch data from CoinGecko Public API
url = "https://api.coingecko.com/api/v3/coins/markets"
params = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 50,
    "page": 1,
    "sparkline": "false"
}

response = requests.get(url, params=params)
if response.status_code == 200:
    data = response.json()
    
    # Convert JSON to Spark DataFrame
    df = spark.createDataFrame(data)
    
    # Add an ingestion timestamp so historical snapshots are preserved
    df_versioned = df.withColumn("ingestion_timestamp", current_timestamp())
    
    # Save as a Unity Catalog managed table using append mode for history retention
    table_name = "default.bronze_coingecko_markets"
    df_versioned.write.format("delta").mode("append").saveAsTable(table_name)
    
    print(f"Successfully appended historical CoinGecko market data to Unity Catalog table: {table_name}")
else:
    raise Exception(f"Failed to fetch data from CoinGecko API: {response.status_code} - {response.text}")
