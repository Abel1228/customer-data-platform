import os
import sys
from pyspark.sql import SparkSession

# Windows fix
os.environ['HADOOP_HOME'] = r"C:\hadoop"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['PATH'] = r"C:\hadoop\bin;" + os.environ['PATH']


def main():
    spark = SparkSession.builder \
        .appName("Bronze ERP Customer AZ12") \
        .master("local[*]") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
        .config("spark.driver.extraLibraryPath", r"C:\hadoop\bin") \
        .config("spark.executor.extraLibraryPath", r"C:\hadoop\bin") \
        .getOrCreate()

    try:
        base_path = os.getcwd()
        input_path = os.path.join(base_path, "data", "erp", "CUST_AZ12.csv")
        output_path = os.path.join(base_path, "data", "bronze", "bronze_erp_cust_az12")

        print(f"Reading from: {input_path}")

        df = spark.read.csv(input_path, header=True, inferSchema=True)

        df.show()

        print(f"Writing to: {output_path}")
        df.write.mode("overwrite").parquet(output_path)

        print("✅ Bronze ERP Customer created!")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()