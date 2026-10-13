from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True)
])

customers_data = [
    (1, "Winston"),
    (2, "Jonathan"),
    (3, "Annabelle"),
    (4, "Marwan"),
    (5, "Khaled")
]

customers_df = spark.createDataFrame(customers_data, schema=customers_schema)

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("cost", IntegerType(), True)
])

orders_data = [
    (1, "2020-07-31", 1, 30),
    (2, "2020-07-30", 2, 40),
    (3, "2020-07-31", 3, 70),
    (4, "2020-07-29", 4, 100),
    (5, "2020-06-10", 1, 1010),
    (6, "2020-08-01", 2, 102),
    (7, "2020-08-01", 3, 111),
    (8, "2020-08-03", 1, 99),
    (9, "2020-08-07", 2, 32),
    (10, "2020-07-15", 1, 2)
]

orders_df = spark.createDataFrame(orders_data, schema=orders_schema)

orders_df.withColumn("rnk", F.dense_rank().over(Window.partitionBy("customer_id").orderBy("order_date"))).filter(F.col("rnk")<=3)\
    .join(customers_df, "customer_id","inner").select("name","customer_id","order_id","order_date").orderBy("name","customer_id",F.col("order_date").desc()).show()