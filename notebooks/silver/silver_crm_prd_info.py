import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when

# Environment setup (Windows)
os.environ['HADOOP_HOME'] = r"C:\hadoop"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['PATH'] = r"C:\hadoop\bin;" + os.environ['PATH']


def main():
    spark = SparkSession.builder \
        .appName("Silver CRM Product Info") \
        .master("local[*]") \
        .getOrCreate()

    try:
        base_path = os.getcwd()

        input_path = os.path.join(base_path, "data", "bronze", "bronze_crm_prd_info")
        output_path = os.path.join(base_path, "data", "silver", "silver_crm_prd_info")

        # Read Bronze data
        df = spark.read.option("header", True).csv(input_path)

        # Rename columns + clean
        df_clean = df.select(
            col("prd_id").cast("int").alias("product_id"),
            col("prd_key").alias("product_key"),
            col("prd_nm").alias("product_name"),
            
            col("prd_cost").cast("float").alias("product_cost"),
            
            # Map product line codes
            when(col("prd_line") == "R", "Road")
            .when(col("prd_line") == "S", "Sport")
            .when(col("prd_line") == "M", "Mountain")
            .otherwise("Unknown")
            .alias("product_line"),

            # Convert dates
            to_date(col("prd_start_dt"), "yyyy-MM-dd").alias("start_date"),
	    to_date(col("prd_end_dt"), "yyyy-MM-dd").alias("end_date")
        )

        df_clean.show()

        # Write Silver data
        df_clean.write.mode("overwrite").parquet(output_path)

        print("✅ Silver product transformation complete!")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()