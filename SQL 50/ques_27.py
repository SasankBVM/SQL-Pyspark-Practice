from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("log_id", IntegerType(), True)
])

data = [
    (1,),
    (2,),
    (3,),
    (7,),
    (8,),
    (10,)
]

df = spark.createDataFrame(data, schema=schema)

window_spec = Window.orderBy("log_id")

df.withColumn("key",F.col("log_id") - F.row_number().over(window_spec)).groupBy("key").agg(F.min("log_id").alias("start_id"), F.max("log_id").alias("end_id")).\
    select("start_id","end_id").show()