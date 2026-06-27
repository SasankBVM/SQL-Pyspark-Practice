from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DateType
from datetime import date

# 1. Initialize Spark Session in local execution mode
spark = SparkSession.builder \
    .appName("SalesDataCreation") \
    .master("local[*]") \
    .getOrCreate()

# ==========================================
# 2. CREATE DOMESTIC SALES DATAFRAME
# ==========================================

# Define Schema
domestic_schema = StructType([
    StructField("sale_id", IntegerType(), False),
    StructField("sale_date", DateType(), True),
    StructField("amount", IntegerType(), True)
])

# Mock Data
domestic_data = [
    (101, date(2026, 6, 1), 500),
    (102, date(2026, 6, 2), 1200),
    (103, date(2026, 6, 3), 750),
    (104, date(2026, 6, 4), 300)  # Shared record example for auditing
]

df_domestic = spark.createDataFrame(data=domestic_data, schema=domestic_schema)

# Define Schema (Matches Domestic)
international_schema = StructType([
    StructField("sale_id", IntegerType(), False),
    StructField("sale_date", DateType(), True),
    StructField("amount", IntegerType(), True)
])

# Mock Data
international_data = [
    (201, date(2026, 6, 1), 3400),
    (202, date(2026, 6, 2), 2100),
    (104, date(2026, 6, 4), 300)  # Shared record example for auditing
]

df_international = spark.createDataFrame(data=international_data, schema=international_schema)

df_domestic.unionAll(df_international).orderBy("sale_id","sale_date").show()
