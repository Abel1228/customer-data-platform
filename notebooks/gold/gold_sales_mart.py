from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col

spark = SparkSession.builder \
    .appName("Gold Sales Mart WITH BRIDGE") \
    .getOrCreate()

# =========================
# LOAD TABLES
# =========================
fact = spark.read.parquet("data/gold/fact_sales")
bridge = spark.read.parquet("data/silver/silver_customer_bridge")

# =========================
# CLEAN
# =========================
fact = fact.withColumn("customer_id", col("customer_id").cast("string"))

# =========================
# JOIN THROUGH BRIDGE
# =========================
df = fact.join(
    bridge,
    fact.customer_id == bridge.erp_customer_id,
    "left"
)

# =========================
# DEBUG
# =========================
print("\n🔍 BRIDGE DEBUG")
df.select(
    "customer_id",
    "crm_customer_id"
).show(10, False)

# =========================
# AGGREGATION
# =========================
gold = df.groupBy(
    "crm_customer_id"
).agg(
    sum("sales_amount").alias("total_revenue"),
    sum("quantity").alias("total_quantity")
)

# =========================
# WRITE OUTPUT
# =========================
gold.write.mode("overwrite").parquet("data/gold/gold_sales_mart")

print("✅ Gold Sales Mart FIXED WITH BRIDGE")