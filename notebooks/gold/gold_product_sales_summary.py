from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count

spark = SparkSession.builder \
    .appName("Gold Product Sales Summary") \
    .getOrCreate()

sales_path = "data/silver/silver_crm_sales_details"
output_path = "data/gold/gold_product_sales_summary"

df_sales = spark.read.parquet(sales_path)

# CLEAN SALES ONLY
df_clean = df_sales.select(
    "product_key",
    "order_id",
    "sales_amount",
    "quantity"
)

# AGGREGATE ONLY FROM SALES
df_gold = df_clean.groupBy("product_key").agg(
    sum("sales_amount").alias("total_revenue"),
    count("order_id").alias("total_orders"),
    sum("quantity").alias("total_quantity")
)

df_gold.write.mode("overwrite").parquet(output_path)

print("✅ Gold Product Sales Summary created (Sales-only model)")