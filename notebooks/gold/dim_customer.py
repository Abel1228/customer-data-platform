from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Dim Customer") \
    .getOrCreate()

customer_path = "data/silver/silver_customer"
output_path = "data/gold/dim_customer"

df = spark.read.parquet(customer_path)

dim_customer = df.select(
    "customer_id",
    "first_name",
    "last_name",
    "gender",
    "marital_status"
)

dim_customer.write.mode("overwrite").parquet(output_path)

print("✅ Dim Customer created")