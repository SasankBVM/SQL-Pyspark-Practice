from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import *


spark = SparkSession.builder \
    .appName("CreateCustomerOrdersData") \
    .master("local[*]") \
    .getOrCreate()

customers_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_name", StringType(), True)
])

customers_data = [
    (1, "Daniel"),
    (2, "Diana"),
    (3, "Elizabeth"),
    (4, "Jhon")  
]

df_customers = spark.createDataFrame(data=customers_data, schema=customers_schema)
orders_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
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

df_orders = spark.createDataFrame(data=orders_data, schema=orders_schema)
df_customers.show()

print("--- Orders Table ---")
df_orders.show()
df_orders.groupBy(col("customer_id")).agg(
    sum(when(col("product_name") == "A",1).otherwise(0)).alias("ProdA"), 
    sum(when(col("product_name") == "B",1).otherwise(0)).alias("ProdB"),
    sum(when(col("product_name") == "C",1).otherwise(0)).alias("ProdC")
    ).filter((col("ProdA")>0) & (col("ProdB")>0) & (col("ProdC")<=0)).select(col("customer_id")).join(df_customers,on="customer_id",how="inner").select(col("customer_id"),col("customer_name")).show(2)
