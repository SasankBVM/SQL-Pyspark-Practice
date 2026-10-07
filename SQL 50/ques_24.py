from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.getOrCreate()

failed_schema = StructType([
    StructField("fail_date", StringType(), True)
])

failed_data = [
    ("2018-12-28",),
    ("2018-12-29",),
    ("2019-01-04",),
    ("2019-01-05",)
]

failed_df = spark.createDataFrame(failed_data, schema=failed_schema)

succeeded_schema = StructType([
    StructField("success_date", StringType(), True)
])

succeeded_data = [
    ("2018-12-30",),
    ("2018-12-31",),
    ("2019-01-01",),
    ("2019-01-02",),
    ("2019-01-03",),
    ("2019-01-06",)
]

succeeded_df = spark.createDataFrame(succeeded_data, schema=succeeded_schema)

failed_df = failed_df.filter(F.col("fail_date").between("2019-01-01", "2019-12-31")).withColumn("fail_date",F.to_date("fail_date","yyyy-MM-dd")).withColumn("period_state", F.lit("failed"))
succeeded_df = succeeded_df.filter(F.col("success_date").between("2019-01-01", "2019-12-31")).withColumn("success_date",F.to_date("success_date","yyyy-MM-dd")).withColumn("period_state",F.lit("succeeded"))

window_spec = Window.partitionBy("period_state").orderBy("fail_date")

failed_df.unionAll(succeeded_df).withColumn("idx",F.col("fail_date") - F.row_number().over(window_spec))\
    .groupBy("idx","period_state").agg(F.min("fail_date").alias("start_date"), F.max("fail_date").alias("end_date")).select("period_state","start_date","end_date")\
    .orderBy("start_date","end_date").show()
