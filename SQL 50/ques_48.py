from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

log_info_schema = StructType([
    StructField("account_id", IntegerType(), True),
    StructField("ip_address", IntegerType(), True),
    StructField("login", StringType(), True),
    StructField("logout", StringType(), True)
])

log_info_data = [
    (1, 1, "2021-02-01 09:00:00", "2021-02-01 09:30:00"),
    (1, 2, "2021-02-01 08:00:00", "2021-02-01 11:30:00"),
    (2, 6, "2021-02-01 20:30:00", "2021-02-01 22:00:00"),
    (2, 7, "2021-02-02 20:30:00", "2021-02-02 22:00:00"),
    (3, 9, "2021-02-01 16:00:00", "2021-02-01 16:59:59"),
    (3, 13, "2021-02-01 17:00:00", "2021-02-01 17:59:59"),
    (4, 10, "2021-02-01 16:00:00", "2021-02-01 17:00:00"),
    (4, 11, "2021-02-01 17:00:00", "2021-02-01 17:59:59")
]

log_info_df = spark.createDataFrame(log_info_data, schema=log_info_schema)
log_info_df.alias("a").join(log_info_df.alias("b"), on = (F.col("a.account_id") == F.col("b.account_id")) & (F.col("a.ip_address")< F.col("b.ip_address"))).\
    filter( (F.col("b.login").between(F.col("a.login"), F.col("a.logout"))) | (F.col("a.login").between(F.col("b.login"), F.col("b.logout")))).\
        select(F.col("a.account_id").alias("account_id")).distinct().show()