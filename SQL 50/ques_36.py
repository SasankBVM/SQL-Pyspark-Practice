from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

person_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("phone_number", StringType(), True)
])

person_data = [
    (3, "Jonathan", "051-1234567"),
    (12, "Elvis", "051-7654321"),
    (1, "Moncef", "212-1234567"),
    (2, "Maroua", "212-6523651"),
    (7, "Meir", "972-1234567"),
    (9, "Rachel", "972-0011100")
]

person_df = spark.createDataFrame(person_data, schema=person_schema)

country_schema = StructType([
    StructField("name", StringType(), True),
    StructField("country_code", StringType(), True)
])

country_data = [
    ("Peru", "051"),
    ("Israel", "972"),
    ("Morocco", "212"),
    ("Germany", "049"),
    ("Ethiopia", "251")
]

country_df = spark.createDataFrame(country_data, schema=country_schema)

calls_schema = StructType([
    StructField("caller_id", IntegerType(), True),
    StructField("callee_id", IntegerType(), True),
    StructField("duration", IntegerType(), True)
])

calls_data = [
    (1, 9, 33),
    (2, 9, 4),
    (1, 2, 59),
    (3, 12, 102),
    (3, 12, 330),
    (12, 3, 5),
    (7, 9, 13),
    (7, 1, 3),
    (9, 7, 1),
    (1, 7, 7)
]

calls_df = spark.createDataFrame(calls_data, schema=calls_schema)

person_with_caller_code = person_df.withColumn("code", F.split("phone_number","-").getItem(0))
all_calls_df_tmp1 = calls_df.join(person_df, on = calls_df["caller_id"] == person_df["id"]).select(F.col("callee_id"), F.col("duration"), F.col("phone_number").alias("ph1")).\
    join(person_df, on = person_df["id"] == calls_df["callee_id"]).select("duration","ph1",F.col("phone_number").alias("ph2")).withColumn("ph1",F.split("ph1","-").getItem(0)).\
        withColumn("ph2", F.split("ph2","-").getItem(0))

all_calls_df_tmp2 = all_calls_df_tmp1.join(country_df, on= all_calls_df_tmp1["ph1"] == country_df["country_code"])\
    .select("duration",F.col("name").alias("country_1"),"ph2").join(country_df,country_df["country_code"] == all_calls_df_tmp1["ph2"]).\
        select("duration", F.col("country_1"), F.col("name").alias("country_2"))

final_df = all_calls_df_tmp2.select("duration","country_1").unionAll(all_calls_df_tmp2.select("duration","country_2"))
global_avg = final_df.select(F.sum("duration")/F.count("*")).collect()[0][0]
final_df.groupBy("country_1").agg(F.avg("duration").alias("avg_duration")).\
    withColumn("avg",F.avg("avg_duration").over(Window.partitionBy(F.lit(None)))).filter(F.col("avg_duration") > global_avg).select(F.col("country_1").alias("country")).show()