from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper

# Initialize Spark
spark = SparkSession.builder \
    .appName("Silver ERP Location") \
    .getOrCreate()

# Paths
input_path = "data/bronze/bronze_erp_loc_a101"
output_path = "data/silver/silver_erp_loc_a101"

# Read Bronze (Parquet)
df = spark.read.parquet(input_path)

# Transformations
df_clean = df.select(
    col("CID").alias("customer_id"),
    upper(trim(col("CNTRY"))).alias("country")
)

# Remove duplicates (FIXED)
df_clean = df_clean.dropDuplicates(["customer_id"])

# Write to Silver
df_clean.write.mode("overwrite").parquet(output_path)

print("✅ Silver ERP Location created!")