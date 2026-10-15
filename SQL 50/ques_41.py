from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

users_schema = StructType([
    StructField("account", IntegerType(), True),
    StructField("name", StringType(), True)
])

users_data = [
    (900001, "Alice"),
    (900002, "Bob"),
    (900003, "Charlie")
]

users_df = spark.createDataFrame(users_data, schema=users_schema)

transactions_schema = StructType([
    StructField("trans_id", IntegerType(), True),
    StructField("account", IntegerType(), True),
    StructField("amount", IntegerType(), True),
    StructField("transacted_on", StringType(), True)
])

transactions_data = [
    (1, 900001, 7000, "2020-08-01"),
    (2, 900001, 7000, "2020-09-01"),
    (3, 900001, -3000, "2020-09-02"),
    (4, 900002, 1000, "2020-09-12"),
    (5, 900003, 6000, "2020-08-07"),
    (6, 900003, 6000, "2020-09-07"),
    (7, 900003, -4000, "2020-09-11")
]

transactions_df = spark.createDataFrame(transactions_data, schema=transactions_schema)

transactions_df.groupBy("account").agg(F.sum("amount").alias("balance")).\
filter(F.col("balance")>10000).join(users_df, "account","inner").select("name","balance").show()