from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("seat_id", IntegerType(), True),
    StructField("free", IntegerType(), True)
])

data = [
    (1, 1),
    (2, 0),
    (3, 1),
    (4, 1),
    (5, 1)
]

df = spark.createDataFrame(data, schema=schema)
df.filter(F.col("free") == 1).withColumn("rn",F.row_number().over(Window.orderBy("seat_id"))).withColumn("diff",F.col("seat_id")-F.col("rn")).\
    withColumn("cnt",F.count("seat_id").over(Window.partitionBy("diff"))).filter(F.col("cnt")>2).orderBy("seat_id").select("seat_id").show()