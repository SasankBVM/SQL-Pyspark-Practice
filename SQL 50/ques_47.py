from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

employees_schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("event_day", StringType(), True),
    StructField("in_time", IntegerType(), True),
    StructField("out_time", IntegerType(), True)
])

employees_data = [
    (1, "2020-11-28", 4, 32),
    (1, "2020-11-28", 55, 200),
    (1, "2020-12-03", 1, 42),
    (2, "2020-11-28", 3, 33),
    (2, "2020-12-09", 47, 74)
]

employees_df = spark.createDataFrame(employees_data, schema=employees_schema)
employees_df.withColumn("time", F.col("out_time") - F.col("in_time")).groupBy("event_day","emp_id").agg(F.sum("time").alias("total_time")).show()