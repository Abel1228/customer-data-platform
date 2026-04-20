from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder \
    .appName("Gold Data Quality Check") \
    .getOrCreate()

def check_table(path, name):
    print(f"\n📊 {name} CHECK")
    print(f"Path: {path}")

    try:
        df = spark.read.parquet(path)

        # Row count (safe)
        row_count = df.count()
        print("\nRow count:", row_count)

        # Schema
        print("\nSchema:")
        df.printSchema()

        # Handle empty table safely
        if row_count == 0:
            print("\n⚠️ WARNING: Table is EMPTY")
            return

        # Sample data
        print("\nSample data:")
        df.show(10, False)

    except AnalysisException as e:
        print("\n❌ ERROR: Cannot read table")
        print("Reason:", str(e))


# =========================
# GOLD TABLE CHECKS
# =========================

check_table("data/gold/fact_sales", "FACT SALES")
check_table("data/gold/dim_customer", "DIM CUSTOMER")
check_table("data/gold/dim_product", "DIM PRODUCT")
check_table("data/gold/gold_sales_mart", "GOLD SALES MART")