from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

person_schema = StructType([
    StructField("personId", IntegerType(), True),
    StructField("lastName", StringType(), True),
    StructField("firstName", StringType(), True)
])

person_data = [
    (1, "Wang", "Allen"),
    (2, "Alice", "Bob")
]

person_df = spark.createDataFrame(person_data, schema=person_schema)

address_schema = StructType([
    StructField("addressId", IntegerType(), True),
    StructField("personId", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True)
])

address_data = [
    (1, 2, "New York City", "New York"),
    (2, 3, "Leetcode", "California")
]

address_df = spark.createDataFrame(address_data, schema=address_schema)

person_df.join(address_df,on="personId",how="left").select(["firstName","lastName","city","state"]).show()