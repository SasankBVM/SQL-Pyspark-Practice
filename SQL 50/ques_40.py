from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

warehouse_schema = StructType([
    StructField("name", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("units", IntegerType(), True)
])

warehouse_data = [
    ("LCHouse1", 1, 1),
    ("LCHouse1", 2, 10),
    ("LCHouse1", 3, 5),
    ("LCHouse2", 1, 2),
    ("LCHouse2", 2, 2),
    ("LCHouse3", 4, 1)
]

warehouse_df = spark.createDataFrame(warehouse_data, schema=warehouse_schema)

products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("Width", IntegerType(), True),
    StructField("Length", IntegerType(), True),
    StructField("Height", IntegerType(), True)
])

products_data = [
    (1, "LC-TV", 5, 50, 40),
    (2, "LC-KeyChain", 5, 5, 5),
    (3, "LC-Phone", 2, 10, 10),
    (4, "LC-T-Shirt", 4, 10, 20)
]

products_df = spark.createDataFrame(products_data, schema=products_schema)

products_df.withColumn("volume", F.col("Width")*F.col("Length")*F.col("Height")).join(warehouse_df, on="product_id",how="inner").\
    groupBy("name").agg((F.sum(F.col("volume")*F.col("units"))).alias("volume")).show()