import os
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Bronze ERP Location") \
        .master("local[*]") \
        .getOrCreate()

    try:
        base_path = os.getcwd()

        input_path = os.path.join(base_path, "data", "erp", "LOC_A101.csv")
        output_path = os.path.join(base_path, "data", "bronze", "bronze_erp_loc_a101")

        print(f"Reading from: {input_path}")

        df = spark.read.csv(input_path, header=True, inferSchema=True)

        df.show()

        print(f"Writing to: {output_path}")

        df.write.mode("overwrite").parquet(output_path)

        print("✅ Bronze ERP Location created!")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()