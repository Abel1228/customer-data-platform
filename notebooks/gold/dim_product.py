from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Dim Product") \
    .getOrCreate()

product_path = "data/silver/silver_crm_prd_info"
output_path = "data/gold/dim_product"

df = spark.read.parquet(product_path)

dim_product = df.select(
    "product_key",
    "product_name",
    "product_line"
).dropDuplicates(["product_key"])

dim_product.write.mode("overwrite").parquet(output_path)

print("✅ Dim Product created")