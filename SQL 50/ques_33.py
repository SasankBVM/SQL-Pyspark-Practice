from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

variables_schema = StructType([
    StructField("name", StringType(), True),
    StructField("value", IntegerType(), True)
])

variables_data = [
    ("x", 66),
    ("y", 77)
]

variables_df = spark.createDataFrame(variables_data, schema=variables_schema)

expressions_schema = StructType([
    StructField("left_operand", StringType(), True),
    StructField("operator", StringType(), True),
    StructField("right_operand", StringType(), True)
])

expressions_data = [
    ("x", ">", "y"),
    ("x", "<", "y"),
    ("x", "=", "y"),
    ("y", ">", "x"),
    ("y", "<", "x"),
    ("x", "=", "x")
]

expressions_df = spark.createDataFrame(expressions_data, schema=expressions_schema)

exp_1 = variables_df.join(expressions_df, on = variables_df["name"] == expressions_df["left_operand"], how="inner").select(F.col("value").alias("value1"),"operator","right_operand")
exp_1.join(variables_df,on = variables_df["name"] == exp_1["right_operand"], how="inner").\
    select("value1","operator",F.col("value").alias("value2")).\
        withColumn("value",
            F.when(F.col("operator") == "=", F.col("value1") == F.col("value2")).\
            when(F.col("operator") == ">", F.col("value1") > F.col("value2")).\
            when(F.col("operator") == "<", F.col("value1") < F.col("value2"))).show()