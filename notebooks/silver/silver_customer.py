from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Silver Unified Customer") \
    .getOrCreate()

# Paths
erp_customer_path = "data/silver/silver_erp_cust_az12"
crm_customer_path = "data/silver/silver_crm_cust_info"
output_path = "data/silver/silver_customer"

# Read data
erp_df = spark.read.parquet(erp_customer_path).alias("erp")
crm_df = spark.read.parquet(crm_customer_path).alias("crm")

# Join
df_joined = erp_df.join(
    crm_df,
    col("erp.customer_id") == col("crm.customer_id"),
    "inner"
)

# Select explicitly to avoid ambiguity
df_final = df_joined.select(
    col("erp.customer_id"),
    col("crm.customer_id_num"),
    col("crm.first_name"),
    col("crm.last_name"),
    col("erp.gender").alias("gender"),
    col("erp.birth_date"),
    col("crm.marital_status")
)

# Remove duplicates
df_final = df_final.dropDuplicates(["customer_id"])

# Write
df_final.write.mode("overwrite").parquet(output_path)

print("✅ Silver Unified Customer created!")