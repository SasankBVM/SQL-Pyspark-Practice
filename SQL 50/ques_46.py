from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

user_visits_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("visit_date", StringType(), True)
])

user_visits_data = [
    (1, "2020-11-28"),
    (1, "2020-10-20"),
    (1, "2020-12-3"),
    (2, "2020-10-5"),
    (2, "2020-12-9"),
    (3, "2020-11-11")
]

user_visits_df = spark.createDataFrame(user_visits_data, schema=user_visits_schema)

window_spec = Window.partitionBy("user_id").orderBy("visit_date")
rank_spec = Window.partitionBy("user_id").orderBy(F.col("diff_days").desc())

user_visits_df.withColumn("immediate_date", F.coalesce(F.lead("visit_date").over(window_spec), F.lit("2021-01-01"))).\
    withColumn("diff_days", F.date_diff("immediate_date","visit_date")).\
        withColumn("rank", F.dense_rank().over(rank_spec)).filter(F.col("rank") == 1).\
            select("user_id",F.col("diff_days").alias("biggest_window")).show()