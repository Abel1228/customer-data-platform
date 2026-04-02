from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, regexp_replace

# Initialize Spark
spark = SparkSession.builder \
    .appName("Silver CRM Customer Info") \
    .getOrCreate()

# Paths
input_path = "data/crm/cust_info.csv"
output_path = "data/silver/silver_crm_cust_info"

# Read CSV
df = spark.read.option("header", True).csv(input_path)

# Clean + standardize
df_clean = df.select(
    col("cst_id").alias("customer_id_num"),
    # remove hyphen mismatch (IMPORTANT)
    regexp_replace(col("cst_key"), "-", "").alias("customer_id"),
    upper(trim(col("cst_firstname"))).alias("first_name"),
    upper(trim(col("cst_lastname"))).alias("last_name"),
    col("cst_marital_status").alias("marital_status"),
    col("cst_gndr").alias("gender")
)

# Remove duplicates
df_clean = df_clean.dropDuplicates(["customer_id"])

# Write
df_clean.write.mode("overwrite").parquet(output_path)

print("✅ Silver CRM Customer created!")