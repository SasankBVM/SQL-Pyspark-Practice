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
    StructField("product_id", IntegerType(), True)
])

orders_data = [
    (1, "2020-07-31", 1, 1),
    (2, "2020-07-30", 2, 2),
    (3, "2020-08-29", 3, 3),
    (4, "2020-07-29", 4, 1),
    (5, "2020-06-10", 1, 2),
    (6, "2020-08-01", 2, 1),
    (7, "2020-08-01", 3, 1),
    (8, "2020-08-03", 1, 2),
    (9, "2020-08-07", 2, 3),
    (10, "2020-07-15", 1, 2)
]

orders_df = spark.createDataFrame(orders_data, schema=orders_schema)

products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("price", IntegerType(), True)
])

products_data = [
    (1, "keyboard", 120),
    (2, "mouse", 80),
    (3, "screen", 600),
    (4, "hard disk", 450)
]

products_df = spark.createDataFrame(products_data, schema=products_schema)

orders_df.join(products_df, on="product_id",how="inner").\
    withColumn("recent_rnk", F.dense_rank().over(Window.partitionBy("product_id").orderBy(F.col("order_date").desc())))\
        .filter(F.col("recent_rnk") == 1).select("product_name","product_id","order_id","order_date").orderBy("product_name","product_id","order_id").show()