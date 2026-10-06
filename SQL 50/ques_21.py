from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("delivery_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("customer_pref_delivery_date", StringType(), True)
])

data = [
    (1, 1, "2019-08-01", "2019-08-02"),
    (2, 5, "2019-08-02", "2019-08-02"),
    (3, 1, "2019-08-11", "2019-08-11"),
    (4, 3, "2019-08-24", "2019-08-26"),
    (5, 4, "2019-08-21", "2019-08-22"),
    (6, 2, "2019-08-11", "2019-08-13")
]

df = spark.createDataFrame(data, schema=schema)
df.select(F.round((F.count(F.when(F.col("order_date") == F.col("customer_pref_delivery_date"),1).otherwise(None)).alias("count")/ (F.count("*")))*100,2).alias("immediate_percentage")).show()