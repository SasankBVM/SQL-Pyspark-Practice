'''
A company stores information about its employees and departments.

Some departments may exist even if no employees are currently assigned to them.

You are given two tables:



Employee Table

╔═════════════╦══════════╗
║ Column Name ║   Type   ║
╠═════════════╬══════════╣
║ employee_id ║   int    ║
║─────────────┼──────────║
║    name     ║ varchar  ║
║─────────────┼──────────║
║   dept_id   ║   int    ║
╚═════════════╩══════════╝
employee_id is the primary key
dept_id indicates the department an employee belongs to


Department Table

╔═════════════╦══════════╗
║ Column Name ║   Type   ║
╠═════════════╬══════════╣
║   dept_id   ║   int    ║
║─────────────┼──────────║
║  dept_name  ║ varchar  ║
╚═════════════╩══════════╝
dept_id is the primary key
Each row represents a department
'''
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Start the Spark session
spark = SparkSession.builder.appName("CompanySchema").getOrCreate()

# 1. Schema for the Department Table
department_schema = StructType([
    StructField("dept_id", IntegerType(), False),     # False means it cannot be empty (Primary Key)
    StructField("dept_name", StringType(), False)
])

# 2. Schema for the Employee Table
employee_schema = StructType([
    StructField("employee_id", IntegerType(), False), # False means it cannot be empty (Primary Key)
    StructField("name", StringType(), False),
    StructField("dept_id", IntegerType(), True)       # True means it can be empty (if no dept is assigned)
])

# --- How to use it to create empty DataFrames ---
df_department = spark.createDataFrame([
    (10, "HR"),
    (20, "IT"),
    (30, "Finance")
], department_schema)
df_employee = spark.createDataFrame([
    (1, "Alice", 10),
    (2, "Bob", 20)
], employee_schema)

df_employee.join(df_department,on="dept_id",how="rightOuter").select(["employee_id","name","dept_id"]).show()