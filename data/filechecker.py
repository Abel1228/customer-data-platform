from pyspark.sql import SparkSession

# Start Spark
spark = SparkSession.builder \
    .appName("File Checker") \
    .getOrCreate()

# =========================
# CHECK GOLD OUTPUT
# =========================
gold_path = "data/gold/gold_sales_summary"

df = spark.read.parquet(gold_path)

print("\n📊 GOLD TABLE CHECK\n")

# Row count
print("Row count:", df.count())

# Schema
print("\nSchema:")
df.printSchema()

# Sample data
print("\nSample data:")
df.show(10, False)