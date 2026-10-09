from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True)
])

customers_data = [
    (1, "Daniel"),
    (2, "Diana"),
    (3, "Elizabeth"),
    (4, "Jhon")
]

customers_df = spark.createDataFrame(customers_data, schema=customers_schema)

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_name", StringType(), True)
])

orders_data = [
    (10, 1, "A"),
    (20, 1, "B"),
    (30, 1, "D"),
    (40, 1, "C"),
    (50, 2, "A"),
    (60, 3, "A"),
    (70, 3, "B"),
    (80, 3, "D"),
    (90, 4, "C")
]

orders_df = spark.createDataFrame(orders_data, schema=orders_schema)

orders_df.groupBy("customer_id").\
    agg(F.count(F.when(F.col("product_name") == "A",1).otherwise(None)).alias("a_count"), F.count(F.when(F.col("product_name") == "B",1).otherwise(None)).alias("b_count"),\
        F.count(F.when(F.col("product_name") == "C",1).otherwise(None)).alias("c_count")).\
        filter((F.col("a_count")>0) & (F.col("b_count")>0) & (F.col("c_count")==0)).\
            join(customers_df, on = "customer_id",how="inner").select("customer_id","customer_name").show()