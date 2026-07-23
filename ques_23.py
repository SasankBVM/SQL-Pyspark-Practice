'''

𝐋𝐢𝐬𝐭 𝐞𝐦𝐩𝐥𝐨𝐲𝐞𝐞𝐬 𝐰𝐡𝐨 𝐞𝐚𝐫𝐧 𝐦𝐨𝐫𝐞 𝐭𝐡𝐚𝐧 𝐭𝐡𝐞𝐢𝐫 𝐝𝐞𝐩𝐚𝐫𝐭𝐦𝐞𝐧𝐭 𝐚𝐯𝐞𝐫𝐚𝐠𝐞.

'''


import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("DepartmentsEmployeesSetup").getOrCreate()

departments_schema = StructType([
    StructField("DeptID", IntegerType(), False),
    StructField("DeptName", StringType(), True)
])

departments_data = [
    (1, "IT"),
    (2, "HR"),
    (3, "Finance")
]

df_departments = spark.createDataFrame(departments_data, departments_schema)

employees_schema = StructType([
    StructField("EmpID", IntegerType(), False),
    StructField("EmpName", StringType(), True),
    StructField("Salary", IntegerType(), True),
    StructField("DeptID", IntegerType(), True)
])

employees_data = [
    (101, "Alice", 70000, 1),
    (102, "Bob", 80000, 1),
    (103, "Charlie", 60000, 1),
    (104, "David", 40000, 2),
    (105, "Eve", 50000, 2),
    (106, "Frank", 90000, 3),
    (107, "Grace", 85000, 3),
    (108, "Hank", 75000, 3)
]

df_employees_data = spark.createDataFrame(employees_data, employees_schema)

window_spec = Window.partitionBy("DeptID")

df_employees_data.withColumn("dept_avg",F.avg("Salary").over(window_spec)).filter(F.col("Salary")>F.col("dept_avg")).\
    select("EmpID","EmpName","Salary","DeptID").show()