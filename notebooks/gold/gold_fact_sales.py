from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Gold Fact Sales") \
    .getOrCreate()

# Paths
sales_path = "data/silver/silver_crm_sales_details"
output_path = "data/gold/fact_sales"

# Load data
df = spark.read.parquet(sales_path)

# =========================
# CLEAN FACT TABLE
# =========================
fact_sales = df.select(
    col("order_id"),
    col("customer_id").cast("string"),
    col("product_key"),
    col("order_date"),
    col("sales_amount").cast("double"),
    col("quantity").cast("int")
)

# =========================
# OPTIONAL: DATA QUALITY CHECKS
# =========================
fact_sales = fact_sales.dropna(subset=["order_id", "customer_id", "product_key"])

# =========================
# WRITE OUTPUT
# =========================
fact_sales.write.mode("overwrite").parquet(output_path)

print("✅ Fact Sales created (clean + standardized)")