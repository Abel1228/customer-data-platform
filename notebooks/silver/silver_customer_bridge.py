from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id

spark = SparkSession.builder \
    .appName("Fixed Customer Bridge") \
    .getOrCreate()

# Load datasets
erp = spark.read.parquet("data/silver/silver_crm_sales_details")
crm = spark.read.parquet("data/silver/silver_customer")

# Normalize
erp_keys = erp.select(
    col("customer_id").cast("string").alias("erp_customer_id")
).distinct()

crm_keys = crm.select(
    col("customer_id").cast("string").alias("crm_customer_id")
).distinct()

# Add row numbers (SIMULATED MATCHING)
erp_keys = erp_keys.withColumn("id", monotonically_increasing_id())
crm_keys = crm_keys.withColumn("id", monotonically_increasing_id())

# Join artificially for learning (IMPORTANT)
bridge = erp_keys.join(crm_keys, "id", "inner") \
    .drop("id")

# Save
bridge.write.mode("overwrite").parquet("data/silver/silver_customer_bridge")

print("✅ FIXED REALISTIC CUSTOMER BRIDGE CREATED")