from datetime import datetime
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# Initialize Spark Session
spark = SparkSession.builder.appName("CoinGeckoBronzeAppendIngestion").getOrCreate()

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
    
    # Add an ingestion timestamp so historical runs are preserved and trackable
    df_versioned = df.withColumn("ingestion_timestamp", current_timestamp())
    
    # Append mode ensures history is kept across every run!
    bronze_output_path = "/mnt/delta/bronze/coingecko_markets"
    df_versioned.write.format("delta").mode("append").save(bronze_output_path)
    print("Successfully appended historical CoinGecko market data to Bronze layer.")
else:
    raise Exception(f"Failed to fetch data from CoinGecko API: {response.status_code} - {response.text}")
