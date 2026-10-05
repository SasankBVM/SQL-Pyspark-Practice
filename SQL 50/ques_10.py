from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

employee_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("departmentId", IntegerType(), True)
])

employee_data = [
    (1, "Joe", 70000, 1),
    (2, "Jim", 90000, 1),
    (3, "Henry", 80000, 2),
    (4, "Sam", 60000, 2),
    (5, "Max", 90000, 1)
]

employee_df = spark.createDataFrame(employee_data, schema=employee_schema).alias("emp")

department_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

department_data = [
    (1, "IT"),
    (2, "Sales")
]

department_df = spark.createDataFrame(department_data, schema=department_schema).alias("dep")

window_spec = Window.partitionBy("departmentId").orderBy(F.col("salary").desc())
employee_df.withColumn("rnk",F.dense_rank().over(window_spec)).filter(F.col("rnk") == 1).\
    join(department_df, on=F.col("emp.departmentId") == F.col("dep.id")).\
        select([F.col("dep.name").alias("Department"), F.col("emp.name").alias("Employee"), F.col("salary").alias("Salary")]).show()