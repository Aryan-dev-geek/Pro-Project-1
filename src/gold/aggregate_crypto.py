from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, sum, current_timestamp, rank, row_number
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("CryptoGoldAggregationEnhanced").getOrCreate()

# Define source (Silver) and target (Gold) tables
silver_table = "db_proproject1_dev_v3.default.silver_crypto_markets"
gold_global_table = "db_proproject1_dev_v3.default.gold_crypto_global_summary"
gold_asset_table = "db_proproject1_dev_v3.default.gold_crypto_asset_trends"
gold_top_gainers_table = "db_proproject1_dev_v3.default.gold_crypto_top_gainers"

print(f"Reading cleaned data from Silver table: {silver_table}")
df_silver = spark.table(silver_table)

# ==========================================
# TABLE 1: Global Market Summary (Over time)
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
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(gold_global_table)

print(f"SUCCESS! Global summary saved to: {gold_global_table}")


# ==========================================
# TABLE 2: Asset-Level Trends (Historical Tracking)
# ==========================================
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
    (col("total_volume") / col("market_cap")).alias("liquidity_ratio"),
    col("last_updated")
).withColumn("processed_at", current_timestamp())

# Keep append mode so history builds up safely for time-series charts
df_asset_trends.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(gold_asset_table)

print(f"SUCCESS! Asset trends appended to: {gold_asset_table}")

# ==========================================
# TABLE 3: Dynamic Top 3 Gainers (For KPI Cards)
# ==========================================
# Window spec to rank coins by 24h performance for the latest ingestion timestamp
latest_timestamp = df_silver.select(max("ingestion_timestamp")).collect()[0][0]
df_latest = df_silver.filter(col("ingestion_timestamp") == latest_timestamp)

window_spec = Window.orderBy(col("price_change_percentage_24h").desc())

df_top_gainers = df_latest.withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") <= 10) \
    .select(
        col("rank"),
        col("name"),
        col("symbol"),
        col("current_price"),
        col("price_change_percentage_24h"),
        col("ingestion_timestamp")
    ).withColumn("processed_at", current_timestamp())

df_top_gainers.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(gold_top_gainers_table)

print(f"SUCCESS! Top 3 dynamic gainers saved to: {gold_top_gainers_table}")
