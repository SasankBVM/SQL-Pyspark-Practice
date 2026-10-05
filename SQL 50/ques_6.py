from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

employees_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

employees_data = [
    (1, "Alice"),
    (7, "Bob"),
    (11, "Meir"),
    (90, "Winston"),
    (3, "Jonathan")
]

employees_df = spark.createDataFrame(employees_data, schema=employees_schema)

employee_uni_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("unique_id", IntegerType(), True)
])

employee_uni_data = [
    (3, 1),
    (11, 2),
    (90, 3)
]

employee_uni_df = spark.createDataFrame(employee_uni_data, schema=employee_uni_schema)

employees_df.join(employee_uni_df, on= "id",how="left").select("unique_id","name").show()