from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("p_id", IntegerType(), True)
])

data = [
    (1, None),
    (2, 1),
    (3, 1),
    (4, 2),
    (5, 2)
]

df = spark.createDataFrame(data, schema=schema)

df.withColumn("type",F.when(F.col("p_id").isNull(), "Root").\
    when((F.col("p_id").isNotNull()) & (F.col("id").isin([row["p_id"] for row in df.where(F.col("p_id").isNotNull()).select("p_id").collect()])), "Inner").\
        otherwise("Leaf")).show()
