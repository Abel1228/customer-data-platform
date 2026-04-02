from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

spark = SparkSession.builder.getOrCreate()

# Input (bronze)
input_path = r"C:\Users\Abel\Desktop\customer-data-platform\data\bronze\bronze_erp_cust_az12"

# Output (silver)
output_path = r"C:\Users\Abel\Desktop\customer-data-platform\data\silver\silver_erp_cust_az12"

# Read bronze data
df = spark.read.parquet(input_path)

# Transform / Clean
df_clean = df \
    .withColumnRenamed("CID", "customer_id") \
    .withColumnRenamed("BDATE", "birth_date") \
    .withColumnRenamed("GEN", "gender") \
    .withColumn("customer_id", trim(col("customer_id"))) \
    .dropDuplicates()

# Write to silver
df_clean.write.mode("overwrite").parquet(output_path)

print("✅ Silver ERP Customer created!")