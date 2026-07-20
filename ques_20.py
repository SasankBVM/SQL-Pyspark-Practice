'''

𝐅𝐢𝐧𝐝 𝐞𝐦𝐩𝐥𝐨𝐲𝐞𝐞𝐬 𝐰𝐡𝐨 𝐡𝐚𝐯𝐞 𝐧𝐞𝐯𝐞𝐫 𝐛𝐞𝐞𝐧 𝐚 𝐦𝐚𝐧𝐚𝐠𝐞𝐫 𝐚𝐧𝐝 𝐡𝐚𝐯𝐞 𝐰𝐨𝐫𝐤𝐞𝐝 𝐢𝐧 𝐦𝐨𝐫𝐞
𝐭𝐡𝐚𝐧 𝐨𝐧𝐞 𝐝𝐞𝐩𝐚𝐫𝐭𝐦𝐞𝐧𝐭.


'''


import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("EmployeesDeptManagersSetup").getOrCreate()

employees_schema = StructType([
    StructField("EmpID", IntegerType(), False),
    StructField("EmpName", StringType(), True)
])

employees_data = [
    (1, "Alice"),
    (2, "Bob"),
    (3, "Charlie"),
    (4, "David"),
    (5, "Eve")
]

df_employees = spark.createDataFrame(employees_data, employees_schema)

departments_schema = StructType([
    StructField("DeptID", IntegerType(), False),
    StructField("DeptName", StringType(), True)
])

departments_data = [
    (101, "HR"),
    (102, "Finance"),
    (103, "IT")
]

df_departments = spark.createDataFrame(departments_data, departments_schema)

emp_dept_schema = StructType([
    StructField("EmpID", IntegerType(), True),
    StructField("DeptID", IntegerType(), True),
    StructField("FromDate", StringType(), True),
    StructField("ToDate", StringType(), True)
])

emp_dept_data = [
    (1, 101, "2021-01-01", "2022-01-01"),
    (1, 102, "2022-02-01", "2023-01-01"),
    (2, 101, "2021-05-01", "2022-06-01"),
    (3, 103, "2022-01-01", "2023-01-01"),
    (3, 101, "2023-02-01", "2024-01-01"),
    (4, 102, "2021-01-01", "2023-01-01"),
    (5, 103, "2021-01-01", "2023-01-01")
]

df_emp_dept = spark.createDataFrame(emp_dept_data, emp_dept_schema)

managers_schema = StructType([
    StructField("EmpID", IntegerType(), False),
    StructField("DeptID", IntegerType(), True)
])

managers_data = [
    (2, 101),
    (5, 103)
]

df_managers = spark.createDataFrame(managers_data, managers_schema)


df_emp_dept.join(df_managers,on=["EmpID"],how="leftanti").\
    groupBy(F.col("EmpID")).agg(F.count("DeptID").alias("dept_worked")).\
        where(F.col("dept_worked") > 1).join(F.broadcast(df_employees),on="EmpID",how="inner").select("EmpID","EmpName").show()