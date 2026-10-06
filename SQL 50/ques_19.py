from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("player_id", IntegerType(), True),
    StructField("device_id", IntegerType(), True),
    StructField("event_date", StringType(), True),
    StructField("games_played", IntegerType(), True)
])

data = [
    (1, 2, "2016-03-01", 5),
    (1, 2, "2016-05-02", 6),
    (1, 3, "2017-06-25", 1),
    (3, 1, "2016-03-02", 0),
    (3, 4, "2018-07-03", 5)
]

df = spark.createDataFrame(data, schema=schema)

window_spec = Window.partitionBy("player_id").orderBy("event_date").rowsBetween(Window.unboundedPreceding,Window.currentRow)

df.withColumn("games_played_so_far",F.sum("games_played").over(window_spec)).select("player_id","event_date","games_played_so_far").show()