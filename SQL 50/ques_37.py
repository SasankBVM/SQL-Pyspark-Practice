from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("country", StringType(), True)
])

customers_data = [
    (1, "Winston", "USA"),
    (2, "Jonathan", "Peru"),
    (3, "Moustafa", "Egypt")
]

customers_df = spark.createDataFrame(customers_data, schema=customers_schema)

product_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("description", StringType(), True),
    StructField("price", IntegerType(), True)
])

product_data = [
    (10, "LC Phone", 300),
    (20, "LC T-Shirt", 10),
    (30, "LC Book", 45),
    (40, "LC Keychain", 2)
]

product_df = spark.createDataFrame(product_data, schema=product_schema)

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("quantity", IntegerType(), True)
])

orders_data = [
    (1, 1, 10, "2020-06-10", 1),
    (2, 1, 20, "2020-07-01", 1),
    (3, 1, 30, "2020-07-08", 2),
    (4, 2, 10, "2020-06-15", 2),
    (5, 2, 40, "2020-07-01", 10),
    (6, 3, 20, "2020-06-24", 2),
    (7, 3, 30, "2020-06-25", 2),
    (9, 3, 30, "2020-05-08", 3)
]

orders_df = spark.createDataFrame(orders_data, schema=orders_schema)

df = orders_df.join(product_df, on="product_id",how="inner").filter((F.year(F.col("order_date")) == "2020") & (F.month("order_date")).isin(6,7)).\
    groupBy("customer_id",F.month("order_date").alias("month")).agg((F.sum(F.col("quantity")*F.col("price"))).alias("spent_amnt"))
    
df.alias("a").join(df.alias("b"), on = (F.col("a.customer_id") == F.col("b.customer_id")) & (F.col("a.month") == 6) & (F.col("b.month") == 7) & (F.col("a.spent_amnt")>=100) & (F.col("b.spent_amnt")>=100) , how="inner")\
    .select(F.col("a.customer_id")).join(customers_df, on="customer_id",how="inner").select("customer_id","name").show()