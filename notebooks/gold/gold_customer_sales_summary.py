from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, round, when

# =========================
# Initialize Spark
# =========================
spark = SparkSession.builder \
    .appName("Gold Sales Summary") \
    .getOrCreate()

# =========================
# Paths
# =========================
sales_path = "data/silver/silver_crm_sales_details"
output_path = "data/gold/gold_sales_summary"

# =========================
# Read Silver Data
# =========================
df_sales = spark.read.parquet(sales_path)

# =========================
# Aggregation
# =========================
df_gold = df_sales.groupBy("customer_id").agg(
    sum("sales_amount").alias("total_spent"),
    count("order_id").alias("total_orders")
)

# =========================
# Average Order Value
# =========================
df_gold = df_gold.withColumn(
    "avg_order_value",
    round(col("total_spent") / col("total_orders"), 2)
)

# =========================
# Customer Segmentation
# =========================
df_gold = df_gold.withColumn(
    "customer_segment",
    when(col("total_spent") >= 10000, "VIP")
    .when(col("total_spent") >= 5000, "Regular")
    .otherwise("Low")
)

# =========================
# Sort by highest spenders
# =========================
df_gold = df_gold.orderBy(col("total_spent").desc())

# =========================
# Write Output
# =========================
df_gold.write.mode("overwrite").parquet(output_path)

print("✅ Gold Sales Summary with segmentation created!")