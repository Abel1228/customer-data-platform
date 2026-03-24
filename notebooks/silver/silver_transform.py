import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when

# --- Environment setup (same as your bronze script) ---
os.environ['HADOOP_HOME'] = r"C:\hadoop"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['PATH'] = r"C:\hadoop\bin;" + os.environ['PATH']


def main():
    spark = SparkSession.builder \
        .appName("Silver Layer Transformation") \
        .master("local[*]") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
        .config("spark.driver.extraLibraryPath", r"C:\hadoop\bin") \
        .config("spark.executor.extraLibraryPath", r"C:\hadoop\bin") \
        .getOrCreate()

    try:
        base_path = os.getcwd()

        # --- Paths ---
        input_path = os.path.join(base_path, "data", "bronze", "bronze_crm_cust_info")
        output_path = os.path.join(base_path, "data", "silver", "silver_crm_cust_info")

        print(f"Reading from Bronze: {input_path}")

        # --- Read Bronze ---
        df = spark.read.csv(input_path, header=True, inferSchema=True)

        # --- Transformations ---
        df_clean = df.select(
            col("cst_id"),
            col("cst_key"),
            trim(col("cst_firstname")).alias("first_name"),
            trim(col("cst_lastname")).alias("last_name"),
            col("cst_marital_status"),
            col("cst_gndr"),
            col("cst_create_date")
        )

        df_clean = df_clean.withColumn(
            "marital_status",
            when(col("cst_marital_status") == "M", "Married")
            .when(col("cst_marital_status") == "S", "Single")
            .otherwise("Unknown")
        )

        print("Transformed Data:")
        df_clean.show()

        # --- Write Silver (Parquet) ---
        print(f"Writing to Silver: {output_path}")

        df_clean.write.mode("overwrite").parquet(output_path)

        print("Silver layer created successfully!")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()