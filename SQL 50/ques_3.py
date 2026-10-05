from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("name", StringType(), True),
    StructField("continent", StringType(), True),
    StructField("area", IntegerType(), True),
    StructField("population", IntegerType(), True),
    StructField("gdp", LongType(), True)
])

data = [
    ("Afghanistan", "Asia", 652230, 25500100, 20343000000),
    ("Albania", "Europe", 28748, 2831741, 12960000000),
    ("Algeria", "Africa", 2381741, 37100000, 188681000000),
    ("Andorra", "Europe", 468, 78115, 3712000000),
    ("Angola", "Africa", 1246700, 20609294, 100990000000)
]

df = spark.createDataFrame(data, schema=schema)

df.filter((F.col("area")>=3000000) | (F.col("population")>=25000000) ).select("name","population","area").show()