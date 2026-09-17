from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, sum, current_timestamp, rank
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("CryptoGoldAggregationExpanded").getOrCreate()

# Define source (Silver) and target (Gold) tables
silver_table = "db_proproject1_dev_v3.default.silver_crypto_markets"
gold_global_table = "db_proproject1_dev_v3.default.gold_crypto_global_summary"
gold_asset_table = "db_proproject1_dev_v3.default.gold_crypto_asset_trends"

print(f"Reading cleaned data from Silver table: {silver_table}")
df_silver = spark.table(silver_table)

# ==========================================
# TABLE 1: Global Market Summary (For Page 1 KPI Cards & Overview Charts)
# ==========================================
df_global_summary = df_silver.groupBy("ingestion_timestamp").agg(
    max("current_price").alias("max_coin_price"),
    min("current_price").alias("min_coin_price"),
    avg("current_price").alias("avg_market_price"),
    sum("market_cap").alias("total_market_cap"),
    sum("total_volume").alias("total_market_volume"),
    max("price_change_percentage_24h").alias("highest_gainer_pct")
).withColumn("processed_at", current_timestamp())

df_global_summary.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(gold_global_table)

print(f"SUCCESS! Global summary saved to: {gold_global_table}")


# ==========================================
# TABLE 2: Asset-Level Trends & Leaderboards (For Page 2 Deep Dives & Slicers)
# ==========================================
# Calculates additional indicators like volume-to-market-cap liquidity ratio
df_asset_trends = df_silver.select(
    col("id"),
    col("symbol"),
    col("name"),
    col("current_price"),
    col("market_cap"),
    col("market_cap_rank"),
    col("total_volume"),
    col("price_change_percentage_24h"),
    col("ingestion_timestamp"),
    # Liquidity indicator: Volume relative to Market Cap
    (col("total_volume") / col("market_cap")).alias("liquidity_ratio"),
    col("last_updated")
).withColumn("processed_at", current_timestamp())

df_asset_trends.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(gold_asset_table)

total_asset_rows = spark.table(gold_asset_table).count()
print(f"SUCCESS! Asset-level trends saved to: {gold_asset_table}. Total rows: {total_asset_rows}")
