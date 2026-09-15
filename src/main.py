from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ProProjectDataPipeline").getOrCreate()

# Simple data test
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["Name", "Age"])

df.show()
print("Databricks Asset Bundle job executed successfully!")
