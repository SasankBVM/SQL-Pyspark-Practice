from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("team_id", IntegerType(), True)
])

data = [
    (1, 8),
    (2, 8),
    (3, 8),
    (4, 7),
    (5, 9),
    (6, 9)
]

df = spark.createDataFrame(data, schema=schema)
window_spec = Window.partitionBy("team_id")
df.withColumn("team_size",F.count("team_id").over(window_spec)).select("employee_id","team_size").orderBy("employee_id").show()