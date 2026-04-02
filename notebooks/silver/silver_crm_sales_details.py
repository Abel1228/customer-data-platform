import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Environment setup (Windows fix)
os.environ['HADOOP_HOME'] = r"C:\hadoop"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['PATH'] = r"C:\hadoop\bin;" + os.environ['PATH']


def main():
    spark = SparkSession.builder \
        .appName("Silver CRM Sales Details Transformation") \
        .master("local[*]") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
        .config("spark.driver.extraLibraryPath", r"C:\hadoop\bin") \
        .config("spark.executor.extraLibraryPath", r"C:\hadoop\bin") \
        .getOrCreate()

    try:
        # Paths
        base_path = os.getcwd()
        input_path = os.path.join(base_path, "data", "bronze", "bronze_crm_sales_details")
        output_path = os.path.join(base_path, "data", "silver", "silver_crm_sales_details")

        print(f"Reading from Bronze: {input_path}")

        # Read Bronze
        df = spark.read.csv(input_path, header=True, inferSchema=True)

        # Transformations
        df = df.select(
            F.col("sls_ord_num").alias("order_id"),
            F.col("sls_prd_key").alias("product_key"),
            F.col("sls_cust_id").cast("int").alias("customer_id"),
            F.col("sls_order_dt").alias("order_date"),
            F.col("sls_ship_dt").alias("ship_date"),
            F.col("sls_due_dt").alias("due_date"),
            F.col("sls_sales").cast("double").alias("sales_amount"),
            F.col("sls_quantity").cast("int").alias("quantity"),
            F.col("sls_price").cast("double").alias("price")
        )

        # ✅ SAFE DATE CONVERSION (fix for your error)
        df = df.withColumn("order_date", F.try_to_date(F.col("order_date").cast("string"), "yyyyMMdd")) \
               .withColumn("ship_date", F.try_to_date(F.col("ship_date").cast("string"), "yyyyMMdd")) \
               .withColumn("due_date", F.try_to_date(F.col("due_date").cast("string"), "yyyyMMdd"))

        print("Transformed Data:")
        df.show()

        # Write to Silver (Parquet)
        print(f"Writing to Silver: {output_path}")
        df.write.mode("overwrite").parquet(output_path)

        print("✅ Silver layer created successfully!")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()