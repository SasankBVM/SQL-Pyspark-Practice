from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

departments_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

departments_data = [
    (1, "Electrical Engineering"),
    (7, "Computer Engineering"),
    (13, "Bussiness Administration")
]

departments_df = spark.createDataFrame(departments_data, schema=departments_schema)

students_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department_id", IntegerType(), True)
])

students_data = [
    (23, "Alice", 1),
    (1, "Bob", 7),
    (5, "Jennifer", 13),
    (2, "John", 14),
    (4, "Jasmine", 77),
    (3, "Steve", 74),
    (6, "Luis", 1),
    (8, "Jonathan", 7),
    (7, "Daiana", 33),
    (11, "Madelynn", 1)
]

students_df = spark.createDataFrame(students_data, schema=students_schema)
departments_df.join(students_df,departments_df.id == students_df.department_id, "right").filter(departments_df["id"].isNull()).\
    select(students_df["id"],students_df["name"]).show()