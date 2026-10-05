from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("email", StringType(), True)
])

data = [
    (1, "a@b.com"),
    (2, "c@d.com"),
    (3, "a@b.com")
]

df = spark.createDataFrame(data, schema=schema)
df.filter(F.col("email").isNotNull()).groupBy("email").agg(F.count("email").alias("frequency")).filter(F.col("frequency")>1).select("email").show()