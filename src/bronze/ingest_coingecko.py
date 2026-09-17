import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# Initialize Spark Session
spark = SparkSession.builder.appName("CoinGeckoIncrementalBronzeIngest").getOrCreate()

# Fetch data from CoinGecko API
url = "https://api.coingecko.com/api/v3/coins/markets"
params = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 100,
    "page": 1,
    "sparkline": "false"
}

response = requests.get(url, params=params)
print("API Status Code:", response.status_code)

if response.status_code == 200:
    data = response.json()
    print(f"Fetched {len(data)} records for incremental snapshot.")
    
    # Load into Pandas first to gracefully handle any minor data type fluctuations
    pdf = pd.DataFrame(data)
    
    # Convert to Spark DataFrame
    df = spark.createDataFrame(pdf)
    
    # Tag with the exact execution timestamp for historical tracking
    df_versioned = df.withColumn("ingestion_timestamp", current_timestamp())
    
    # Target our established v2 table
    table_name = "db_proproject1_dev_v3.default.bronze_coingecko_markets_v2"
    
    # APPEND mode ensures historical data is preserved every 15 minutes!
    df_versioned.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(table_name)
    
    total_count = spark.table(table_name).count()
    print(f"SUCCESS! Appended new snapshot. Total rows in history: {total_count}")
else:
    raise Exception(f"Failed to fetch data from CoinGecko API: {response.status_code} - {response.text}")
