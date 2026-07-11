from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import col,greatest,least,count,sum

# Start the Spark session
spark = SparkSession.builder.appName("CallsData").getOrCreate()

# ==========================================
# CALLS SCHEMA
# ==========================================
calls_schema = StructType([
    StructField("from_id", IntegerType(), False),
    StructField("to_id", IntegerType(), False),
    StructField("duration", IntegerType(), False)
])

# ==========================================
# CALLS DATA
# ==========================================
calls_data = [
    (1, 2, 59),
    (2, 1, 11),
    (1, 3, 20),
    (3, 4, 100),
    (3, 4, 200),
    (3, 4, 200),
    (4, 3, 499)
]

df_calls = spark.createDataFrame(calls_data, calls_schema)

# ==========================================
# Show the results
# ==========================================
print("Calls DataFrame:")
df_calls.withColumn("gr1",least(col("from_id"),col("to_id"))).\
withColumn("gr2",greatest(col("from_id"),col("to_id"))).\
groupBy(col("gr1"),col("gr2")).agg(count("from_id").alias("call_count"),sum("duration").alias("total_duration")).\
select([col("gr1").alias("from_id"),col("gr2").alias("to_id"),"call_count","total_duration"]).show()