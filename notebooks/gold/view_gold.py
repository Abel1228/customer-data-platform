from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("View Gold Data") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.parquet("data/gold/gold_customer_summary")

df.show()

spark.stop()