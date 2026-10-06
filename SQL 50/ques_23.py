from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

teams_schema = StructType([
    StructField("team_id", IntegerType(), True),
    StructField("team_name", StringType(), True)
])

teams_data = [
    (10, "Leetcode FC"),
    (20, "NewYork FC"),
    (30, "Atlanta FC"),
    (40, "Chicago FC"),
    (50, "Toronto FC")
]

teams_df = spark.createDataFrame(teams_data, schema=teams_schema)

matches_schema = StructType([
    StructField("match_id", IntegerType(), True),
    StructField("host_team", IntegerType(), True),
    StructField("guest_team", IntegerType(), True),
    StructField("host_goals", IntegerType(), True),
    StructField("guest_goals", IntegerType(), True)
])

matches_data = [
    (1, 10, 20, 3, 0),
    (2, 30, 10, 2, 2),
    (3, 10, 50, 5, 1),
    (4, 20, 30, 1, 0),
    (5, 50, 30, 1, 0)
]

matches_df = spark.createDataFrame(matches_data, schema=matches_schema)

host_matches = matches_df.withColumn("points", F.when(F.col("host_goals") > F.col("guest_goals"), F.lit(3)).when(F.col("host_goals") == F.col("guest_goals"), F.lit(1))).\
               groupBy(F.col("host_team").alias("team_id")).agg(F.sum("points").alias("num_points"))
guest_matches = matches_df.withColumn("points", F.when(F.col("host_goals") < F.col("guest_goals"), F.lit(3)).when(F.col("host_goals") == F.col("guest_goals"), F.lit(1))).\
                groupBy(F.col("guest_team")).agg(F.sum("points").alias("num_points"))
points_df = host_matches.unionAll(guest_matches)

teams_df.join(points_df,on="team_id",how="left").groupBy(["team_id","team_name"]).agg(F.sum("num_points").alias("num_points")).select("team_id","team_name",F.ifnull(F.col("num_points"),F.lit(0)).alias("num_points")).orderBy(F.col("num_points").desc(), "team_id").show()