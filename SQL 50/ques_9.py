from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

customers_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

customers_data = [
    (1, "Joe"),
    (2, "Henry"),
    (3, "Sam"),
    (4, "Max")
]

customers_df = spark.createDataFrame(customers_data, schema=customers_schema)

orders_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("customerId", IntegerType(), True)
])

orders_data = [
    (1, 3),
    (2, 1)
]

orders_df = spark.createDataFrame(orders_data, schema=orders_schema)

customers_df.join(orders_df,on=customers_df["id"] == orders_df["customerId"],how="leftanti").select(F.col("name").alias("Customers")).show()
