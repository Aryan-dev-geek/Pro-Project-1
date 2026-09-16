from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, sum, current_timestamp

# Initialize Spark Session
spark = SparkSession.builder.appName("CryptoGoldAggregation").getOrCreate()

# Define source (Silver) and target (Gold) tables
silver_table = "db_proproject1_dev_v3.default.silver_crypto_markets"
gold_table = "db_proproject1_dev_v3.default.gold_crypto_summary"

print(f"Reading cleaned data from Silver table: {silver_table}")
df_silver = spark.table(silver_table)

# Example Gold Business Aggregation: 
# Summarize market metrics grouped by ingestion timestamp (each 15-min snapshot batch)
df_gold = df_silver.groupBy("ingestion_timestamp").agg(
    max("current_price").alias("max_coin_price"),
    min("current_price").alias("min_coin_price"),
    avg("current_price").alias("avg_market_price"),
    sum("market_cap").alias("total_market_cap"),
    sum("total_volume").alias("total_market_volume"),
    max("price_change_percentage_24h").alias("highest_gainer_pct")
).withColumn("processed_at", current_timestamp())

# Write aggregated metrics to the Gold Delta table in append mode to maintain historical trends
df_gold.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(gold_table)

total_gold_rows = spark.table(gold_table).count()
print(f"SUCCESS! Aggregated gold data saved to: {gold_table}. Total summary rows: {total_gold_rows}")
