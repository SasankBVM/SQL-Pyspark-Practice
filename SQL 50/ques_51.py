from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType

spark = SparkSession.builder.getOrCreate()

products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("store1", IntegerType(), True),
    StructField("store2", IntegerType(), True),
    StructField("store3", IntegerType(), True)
])

products_data = [
    (0, 95, 100, 105),
    (1, 70, None, 80)
]

products_df = spark.createDataFrame(products_data, schema=products_schema)

products_df.select("product_id", F.lit("store1").alias("store"), F.col("store1").alias("price")).unionAll(products_df.select("product_id", F.lit("store2").alias("store"), "store2")).\
    unionAll(products_df.select("product_id", F.lit("store3").alias("store"), "store3")).filter(F.col("price").isNotNull()).show()