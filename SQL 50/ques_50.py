from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

players_schema = StructType([
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True)
])

players_data = [
    (1, "Nadal"),
    (2, "Federer"),
    (3, "Novak")
]

players_df = spark.createDataFrame(players_data, schema=players_schema)

championships_schema = StructType([
    StructField("year", IntegerType(), True),
    StructField("Wimbledon", IntegerType(), True),
    StructField("Fr_open", IntegerType(), True),
    StructField("US_open", IntegerType(), True),
    StructField("Au_open", IntegerType(), True)
])

championships_data = [
    (2018, 1, 1, 1, 1),
    (2019, 1, 1, 2, 2),
    (2020, 2, 1, 2, 2)
]

championships_df = spark.createDataFrame(championships_data, schema=championships_schema)

championships_df.select(F.col("Wimbledon").alias("player_id")).unionAll(championships_df.select(F.col("Fr_open").alias("player_id"))).\
    unionAll(championships_df.select(F.col("US_open").alias("player_id"))).unionAll(championships_df.select(F.col("Au_open").alias("player_id"))).\
        groupBy("player_id").agg(F.count("player_id").alias("grand_slams_count")).join(players_df, on="player_id").\
            select("player_id","player_name","grand_slams_count").show()