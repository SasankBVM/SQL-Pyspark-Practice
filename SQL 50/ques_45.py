from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType

spark = SparkSession.builder.getOrCreate()

calls_schema = StructType([
    StructField("from_id", IntegerType(), True),
    StructField("to_id", IntegerType(), True),
    StructField("duration", IntegerType(), True)
])

calls_data = [
    (1, 2, 59),
    (2, 1, 11),
    (1, 3, 20),
    (3, 4, 100),
    (3, 4, 200),
    (3, 4, 200),
    (4, 3, 499)
]

calls_df = spark.createDataFrame(calls_data, schema=calls_schema)
# Method 1
# calls_df.withColumn("person1", F.least("from_id","to_id")).withColumn("person2", F.greatest("from_id","to_id")).select("person1","person2","duration").\
#     groupBy("person1","person2").agg(F.count("person1").alias("call_count"),F.sum("duration").alias("total_duration")).show()

# Method 2
calls_df.withColumn("person1", F.when(F.col("from_id") < F.col("to_id"), F.lit(F.col("from_id"))).otherwise(F.lit(F.col("to_id")))).\
    withColumn("person2", F.when(F.col("from_id") > F.col("to_id"), F.lit(F.col("from_id"))).otherwise(F.lit(F.col("to_id")))).\
        select("person1","person2","duration").groupBy("person1","person2").agg(F.count("person1").alias("call_count"), F.sum("duration").alias("total_duration")).show()
