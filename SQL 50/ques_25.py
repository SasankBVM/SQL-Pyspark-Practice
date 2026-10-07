from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType

spark = SparkSession.builder.getOrCreate()

friendship_schema = StructType([
    StructField("user1_id", IntegerType(), True),
    StructField("user2_id", IntegerType(), True)
])

friendship_data = [
    (1, 2),
    (1, 3),
    (1, 4),
    (2, 3),
    (2, 4),
    (2, 5),
    (6, 1)
]

friendship_df = spark.createDataFrame(friendship_data, schema=friendship_schema)

likes_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("page_id", IntegerType(), True)
])

likes_data = [
    (1, 88),
    (2, 23),
    (3, 24),
    (4, 56),
    (5, 11),
    (6, 33),
    (2, 77),
    (3, 77),
    (6, 88)
]

likes_df = spark.createDataFrame(likes_data, schema=likes_schema)

friends_of_u1 = friendship_df.withColumn("user_id",F.when(
            F.col("user1_id") == 1, F.col("user2_id")).when(F.col("user2_id") == 1, F.col("user1_id"))
).filter(F.col("user_id").isNotNull()).select("user_id")

posts_liked_by_user_1 = likes_df.filter(F.col("user_id") == 1).select(F.col("page_id"))

likes_df.join(friends_of_u1,on="user_id",how="inner").filter(~F.col("page_id").isin(posts_liked_by_user_1.select("page_id"))).select("page_id").distinct().orderBy("page_id").show()