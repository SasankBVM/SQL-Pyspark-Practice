from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

tv_program_schema = StructType([
    StructField("program_date", StringType(), True),
    StructField("content_id", IntegerType(), True),
    StructField("channel", StringType(), True)
])

tv_program_data = [
    ("2020-06-10 08:00", 1, "LC-Channel"),
    ("2020-05-11 12:00", 2, "LC-Channel"),
    ("2020-05-12 12:00", 3, "LC-Channel"),
    ("2020-05-13 14:00", 4, "Disney Ch"),
    ("2020-06-18 14:00", 4, "Disney Ch"),
    ("2020-07-15 16:00", 5, "Disney Ch")
]

tv_program_df = spark.createDataFrame(tv_program_data, schema=tv_program_schema)

content_schema = StructType([
    StructField("content_id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("Kids_content", StringType(), True),
    StructField("content_type", StringType(), True)
])

content_data = [
    (1, "Leetcode Movie", "N", "Movies"),
    (2, "Alg. for Kids", "Y", "Series"),
    (3, "Database Sols", "N", "Series"),
    (4, "Aladdin", "Y", "Movies"),
    (5, "Cinderella", "Y", "Movies")
]

content_df = spark.createDataFrame(content_data, schema=content_schema)
content_df.filter( (F.col("Kids_content") == "Y")).join(tv_program_df, on = "content_id", how="inner").\
    filter((F.month(F.col("program_date")) == 6) & (F.year("program_date")=="2020")).select("title").distinct().show()