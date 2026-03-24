import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

os.environ['HADOOP_HOME'] = r"C:\hadoop"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['PATH'] = r"C:\hadoop\bin;" + os.environ['PATH']

def main():
    spark = SparkSession.builder \
        .appName("Gold Layer Aggregation") \
        .master("local[*]") \
        .getOrCreate()

    base_path = os.getcwd()

    input_path = os.path.join(base_path, "data", "silver", "silver_crm_cust_info")
    output_path = os.path.join(base_path, "data", "gold", "gold_customer_summary")

    print(f"Reading from Silver: {input_path}")

    df = spark.read.parquet(input_path)

    # Aggregation
    gold_df = df.groupBy("marital_status").agg(
        count("*").alias("customer_count")
    )

    print("Gold Data:")
    gold_df.show()

    print(f"Writing to Gold: {output_path}")
    gold_df.write.mode("overwrite").parquet(output_path)

    print("Gold layer created successfully!")

    spark.stop()

if __name__ == "__main__":
    main()