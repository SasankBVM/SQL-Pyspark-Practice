from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("student_id", IntegerType(), True),
    StructField("course_id", IntegerType(), True),
    StructField("grade", IntegerType(), True)
])

data = [
    (2, 2, 95),
    (2, 3, 95),
    (1, 1, 90),
    (1, 2, 99),
    (3, 1, 80),
    (3, 2, 75),
    (3, 3, 82)
]

df = spark.createDataFrame(data, schema=schema)

window_spec = Window.partitionBy("student_id").orderBy(F.col("grade").desc(),"course_id")

df.withColumn("rank", F.dense_rank().over(window_spec)).filter(F.col("rank") == 1).select("student_id","course_id","grade").show()