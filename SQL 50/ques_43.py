from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True)
])

customer_data = [
    (101, "Alice"),
    (102, "Bob"),
    (103, "Charlie")
]

customer_df = spark.createDataFrame(customer_data, schema=customer_schema)

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("sale_date", StringType(), True),
    StructField("order_cost", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("seller_id", IntegerType(), True)
])

orders_data = [
    (1, "2020-03-01", 1500, 101, 1),
    (2, "2020-05-25", 2400, 102, 2),
    (3, "2019-05-25", 800, 101, 3),
    (4, "2020-09-13", 1000, 103, 2),
    (5, "2019-02-11", 700, 101, 2)
]

orders_df = spark.createDataFrame(orders_data, schema=orders_schema)

seller_schema = StructType([
    StructField("seller_id", IntegerType(), True),
    StructField("seller_name", StringType(), True)
])

seller_data = [
    (1, "Daniel"),
    (2, "Elizabeth"),
    (3, "Frank")
]

seller_df = spark.createDataFrame(seller_data, schema=seller_schema)

orders_df.alias("a").filter(F.year(F.col("sale_date"))==2020).\
    select("seller_id").join(seller_df.alias("b"), on="seller_id", how="right").\
        filter(F.col("a.seller_id").isNull()).select("seller_name").orderBy(F.col("seller_name").desc()).show()