from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import when,coalesce,col

# Start the Spark session
spark = SparkSession.builder.appName("HRSynchronization").getOrCreate()

# ==========================================
# 1. EMPLOYEES DATA & SCHEMA
# ==========================================
employees_schema = StructType([
    StructField("employee_id", IntegerType(), False),
    StructField("name", StringType(), False)
])

employees_data = [
    (1, "Aarav"),
    (2, "Neha"),
    (3, "Rohan")
]

df_employees = spark.createDataFrame(employees_data, employees_schema)

# ==========================================
# 2. SALARIES DATA & SCHEMA
# ==========================================
salaries_schema = StructType([
    StructField("employee_id", IntegerType(), False),
    StructField("salary", IntegerType(), False)
])

salaries_data = [
    (1, 50000),
    (4, 65000)
]

df_salaries = spark.createDataFrame(salaries_data, salaries_schema)

# ==========================================
# Show the base DataFrames
# ==========================================
df_employees.join(df_salaries,df_employees["employee_id"] == df_salaries["employee_id"],"full").select(coalesce(df_employees["employee_id"],df_salaries["employee_id"]).alias("employee_id")
                                                                                                       ,col("name"),col("salary")).orderBy(col("employee_id")).show()