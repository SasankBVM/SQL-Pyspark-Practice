from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType

spark = SparkSession.builder.getOrCreate()

customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("year", IntegerType(), True),
    StructField("revenue", IntegerType(), True)
])

customers_data = [
    (1, 2018, 50),
    (1, 2021, 30),
    (1, 2020, 70),
    (2, 2021, -50),
    (3, 2018, 10),
    (3, 2016, 50),
    (4, 2021, 20)
]

customers_df = spark.createDataFrame(customers_data, schema=customers_schema)
customers_df.filter((F.col("year") == 2021) & (F.col("revenue") >0)).select("customer_id").show()