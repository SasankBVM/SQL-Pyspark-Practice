from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("low_fats", StringType(), True),
    StructField("recyclable", StringType(), True)
])

data = [
    (0, "Y", "N"),
    (1, "Y", "Y"),
    (2, "N", "Y"),
    (3, "Y", "Y"),
    (4, "N", "N")
]

df = spark.createDataFrame(data, schema=schema)
df.filter((F.col("low_fats") == "Y") & (F.col("recyclable") == "Y")).show()