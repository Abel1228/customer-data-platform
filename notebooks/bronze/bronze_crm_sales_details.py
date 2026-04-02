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
        .appName("Bronze CRM Sales Details") \
        .master("local[*]") \
        .config("spark.driver.extraLibraryPath", r"C:\hadoop\bin") \
        .config("spark.executor.extraLibraryPath", r"C:\hadoop\bin") \
        .getOrCreate()

    try:
        base_path = os.getcwd()

        input_path = os.path.join(base_path, "data", "crm", "sales_details.csv")
        output_path = os.path.join(base_path, "data", "bronze", "bronze_crm_sales_details")

        print(f"Reading from: {input_path}")

        df = spark.read.csv(input_path, header=True, inferSchema=True)

        df.show()

        print(f"Writing to: {output_path}")

        df.write.mode("overwrite").csv(output_path, header=True)

        print("✅ Bronze sales ingestion complete!")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()