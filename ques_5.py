'''
A company stores employee details in an Employees table. After a performance review, the HR team decided to give a salary hike to employees working in a specific department.



Table: Employees

╔═════════════╦══════════╗
║ Column Name ║   Type   ║
╠═════════════╬══════════╣
║ employee_id ║   int    ║
║─────────────┼──────────║
║    name     ║ varchar  ║
║─────────────┼──────────║
║ department  ║ varchar  ║
║─────────────┼──────────║
║   salary    ║   int    ║
╚═════════════╩══════════╝
employee_id is the primary key.
Each row represents one employee.


Existing Data

╔═════════════╦══════════╦═════════════╦══════════╗
║ employee_id ║   name   ║ department  ║  salary  ║
╠═════════════╬══════════╬═════════════╬══════════╣
║      1      ║  Aarav   ║ Engineering ║  70000   ║
║─────────────┼──────────┼─────────────┼──────────║
║      2      ║   Neha   ║     HR      ║  50000   ║
║─────────────┼──────────┼─────────────┼──────────║
║      3      ║  Rohan   ║ Engineering ║  80000   ║
║─────────────┼──────────┼─────────────┼──────────║
║      4      ║  Meera   ║   Finance   ║  65000   ║
╚═════════════╩══════════╩═════════════╩══════════╝
Increase the salary of all employees in the Engineering department by 10,000. Only employees belonging to Engineering should be updated.
'''
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col,when

# 1. Initialize Spark Session in local execution mode
spark = SparkSession.builder \
    .appName("EmployeeDataSetup") \
    .master("local[*]") \
    .getOrCreate()

# Define Schema
employees_schema = StructType([
    StructField("employee_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True)
])

# Raw Data from the image
employees_data = [
    (1, "Aarav", "Engineering", 70000),
    (2, "Neha", "HR", 50000),
    (3, "Rohan", "Engineering", 80000),
    (4, "Meera", "Finance", 65000)
]

# Create DataFrame
df_employees = spark.createDataFrame(data=employees_data, schema=employees_schema)

df_employees = df_employees.withColumn("salary",when(col("department") == "Engineering", col("salary")+10000).otherwise(col("salary")))
