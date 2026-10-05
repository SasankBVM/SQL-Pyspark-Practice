from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("tweet_id", IntegerType(), True),
    StructField("content", StringType(), True)
])

data = [
    (1, "Let us Code"),
    (2, "More than fifteen chars are here!")
]

df = spark.createDataFrame(data, schema=schema)
df.filter(F.length(F.col("content")) >=15).select("tweet_id").show()