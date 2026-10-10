from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

users_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

users_data = [
    (1, "Alice"),
    (2, "Bob"),
    (3, "Alex"),
    (4, "Donald"),
    (7, "Lee"),
    (13, "Jonathan"),
    (19, "Elvis")
]

users_df = spark.createDataFrame(users_data, schema=users_schema)

rides_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("distance", IntegerType(), True)
])

rides_data = [
    (1, 1, 120),
    (2, 2, 317),
    (3, 3, 222),
    (4, 7, 100),
    (5, 13, 312),
    (6, 19, 50),
    (7, 7, 120),
    (8, 19, 400),
    (9, 7, 230)
]

rides_df = spark.createDataFrame(rides_data, schema=rides_schema)

rides_df.groupBy("user_id").agg(F.sum("distance").alias("travelled_distance")).\
    join(users_df,rides_df["user_id"] == users_df["id"],"right").select("name",F.coalesce("travelled_distance",F.lit(0)).alias("travelled_distance")).orderBy(F.col("travelled_distance").desc(),"name").show()