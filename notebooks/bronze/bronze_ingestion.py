import os
import sys
from pyspark.sql import SparkSession

# 1. Force environment variables within the script to ensure the JVM sees them
os.environ['HADOOP_HOME'] = r"C:\hadoop"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Add bin to the Windows PATH for this session
os.environ['PATH'] = r"C:\hadoop\bin;" + os.environ['PATH']

def main():
    # 2. Initialize SparkSession with explicit native library configs
    spark = SparkSession.builder \
        .appName("Bronze Layer Ingestion") \
        .master("local[*]") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
        .config("spark.driver.extraLibraryPath", r"C:\hadoop\bin") \
        .config("spark.executor.extraLibraryPath", r"C:\hadoop\bin") \
        .getOrCreate()

    try:
        # 3. Define Paths (using absolute paths helps avoid 'file not found' on Windows)
        base_path = os.getcwd()
        input_path = os.path.join(base_path, "data", "crm", "cust_info.csv")
        output_path = os.path.join(base_path, "data", "bronze", "bronze_crm_cust_info")

        print(f"Reading from: {input_path}")
        
        # 4. Read Data
        df = spark.read.csv(input_path, header=True, inferSchema=True)

        # 5. Show Data (This usually works even without winutils)
        df.show()

        # 6. Write Data (This is where winutils/hadoop.dll is required)
        print(f"Writing to: {output_path}...")
        df.write.mode("overwrite").csv(output_path, header=True)

        print("Ingestion Done successfully!")

    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()