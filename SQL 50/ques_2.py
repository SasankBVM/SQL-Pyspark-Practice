from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("referee_id", IntegerType(), True)
])

data = [
    (1, "Will", None),
    (2, "Jane", None),
    (3, "Alex", 2),
    (4, "Bill", None),
    (5, "Zack", 1),
    (6, "Mark", 2)
]

df = spark.createDataFrame(data, schema=schema)
df.filter((F.col("referee_id")!=2) | (F.col("referee_id").isNull())).select("name").show()