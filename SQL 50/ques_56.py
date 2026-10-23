from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

logins_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("time_stamp", StringType(), True)
])

logins_data = [
    (6, "2020-06-30 15:06:07"),
    (6, "2021-04-21 14:06:06"),
    (6, "2019-03-07 00:18:15"),
    (8, "2020-02-01 05:10:53"),
    (8, "2020-12-30 00:46:50"),
    (2, "2020-01-16 02:49:50"),
    (2, "2019-08-25 07:59:08"),
    (14, "2019-07-14 09:00:00"),
    (14, "2021-01-06 11:59:59")
]

logins_df = spark.createDataFrame(logins_data, schema=logins_schema)
logins_df.withColumn("year", F.year(F.to_date("time_stamp"))).filter(F.col("year") == 2020).groupBy("user_id").agg(F.max("time_stamp").alias("last_stamp")).show()