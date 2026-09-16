from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_timestamp

## Initialize Spark Session
spark = SparkSession.builder.appName("CryptoSilverCleaning").getOrCreate()

# Define source (Bronze v2) and target (Silver) tables
bronze_table = "db_proproject1_dev_v3.default.bronze_coingecko_markets_v2"
silver_table = "db_proproject1_dev_v3.default.silver_crypto_markets"

print(f"Reading raw data from Bronze table: {bronze_table}")
df_bronze = spark.table(bronze_table)

# 1. Drop exact duplicate rows based on crypto id and ingestion timestamp
df_deduped = df_bronze.dropDuplicates(["id", "ingestion_timestamp"])

# 2. Select, clean, and explicitly cast key metrics to standard data types
df_cleaned = df_deduped.select(
    col("id").cast("string"),
    col("symbol").cast("string"),
    col("name").cast("string"),
    col("current_price").cast("double"),
    col("market_cap").cast("double"),
    col("market_cap_rank").cast("int"),
    col("total_volume").cast("double"),
    col("high_24h").cast("double"),
    col("low_24h").cast("double"),
    col("price_change_24h").cast("double"),
    col("price_change_percentage_24h").cast("double"),
    col("circulating_supply").cast("double"),
    col("total_supply").cast("double"),
    col("ingestion_timestamp"),
    to_timestamp(col("last_updated")).alias("last_updated")
)

# 3. Filter out rows where crucial metrics like price or market cap are null or negative
df_filtered = df_cleaned.filter(
    col("current_price").isNotNull() & 
    (col("current_price") >= 0) & 
    col("market_cap").isNotNull()
)

# 4. Write cleaned data to the Silver Delta table in append mode to preserve history
df_filtered.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(silver_table)

total_silver_rows = spark.table(silver_table).count()
print(f"SUCCESS! Cleaned data appended to Silver table: {silver_table}. Total rows: {total_silver_rows}")
