from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col

# Start the Spark session
spark = SparkSession.builder.appName("EmployeeSalaries").getOrCreate()

# ==========================================
# EMPLOYEES SCHEMA
# ==========================================
employees_schema = StructType([
    StructField("employee_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("salary", IntegerType(), False),
    StructField("manager_id", IntegerType(), True)  # True allows None/NULL for the top manager
])

# ==========================================
# EMPLOYEES DATA
# ==========================================
employees_data = [
    (1, "Ritesh", 90000, None),
    (2, "Ananya", 95000, 1),
    (3, "Mohit", 85000, 1),
    (4, "Kunal", 97000, 2)
]

df_employees = spark.createDataFrame(employees_data, employees_schema)

df_a = df_employees.alias("a")
df_b = df_employees.alias("b")

df_a.join(df_b,col("a.employee_id") == col("b.manager_id"),how="inner").\
    where(col("a.salary")<col("b.salary")).select(col("b.name")).show()