import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import DoubleType

spark = SparkSession.builder.appName("CoinGeckoBronzeFix").getOrCreate()

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
    df = spark.createDataFrame(data)
    
    # Explicitly cast numeric columns to Double to prevent type mismatches
    numeric_cols = ["current_price", "market_cap", "total_volume", "high_24h", "low_24h"]
    for c in numeric_cols:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(DoubleType()))

    df_versioned = df.withColumn("ingestion_timestamp", current_timestamp())
    
    table_name = "db_proproject1_dev_v3.default.bronze_coingecko_markets"
    
    # Overwrite schema once to clear out the old LongType definition conflict
    df_versioned.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)
        
    print(f"Successfully re-created and initialized table schema for: {table_name}")
else:
    raise Exception(f"Failed to fetch data: {response.status_code} - {response.text}")
