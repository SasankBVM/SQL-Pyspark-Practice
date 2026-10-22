from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

transactions_schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("day", StringType(), True),
    StructField("amount", IntegerType(), True)
])

transactions_data = [
    (8, "2021-4-3 15:57:28", 57),
    (9, "2021-4-28 08:47:25", 21),
    (1, "2021-4-29 13:28:30", 58),
    (5, "2021-4-28 16:39:59", 40),
    (6, "2021-4-29 23:39:28", 58)
]

transactions_df = spark.createDataFrame(transactions_data, schema=transactions_schema)
window_spec = Window.partitionBy(F.try_to_date("day")).orderBy(F.col("amount").desc())

transactions_df.withColumn("rnk",F.dense_rank().over(window_spec)).filter(F.col("rnk") == 1).orderBy(F.col("transaction_id")).select("transaction_id").show()