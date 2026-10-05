from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("order_number", IntegerType(), True),
    StructField("customer_number", IntegerType(), True)
])

data = [
    (1, 1),
    (2, 2),
    (3, 3),
    (4, 3)
]

df = spark.createDataFrame(data, schema=schema)
df.groupBy("customer_number").agg(F.count("order_number").alias("num_of_orders")).orderBy(F.col("num_of_orders").desc()).select("customer_number").limit(1).show()