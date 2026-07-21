'''

𝐈𝐝𝐞𝐧𝐭𝐢𝐟𝐲 𝐜𝐮𝐬𝐭𝐨𝐦𝐞𝐫𝐬 𝐰𝐡𝐨 𝐩𝐮𝐫𝐜𝐡𝐚𝐬𝐞𝐝 𝐩𝐫𝐨𝐝𝐮𝐜𝐭𝐬 𝐟𝐫𝐨𝐦 𝐚𝐥𝐥 𝐚𝐯𝐚𝐢𝐥𝐚𝐛𝐥𝐞 𝐜𝐚𝐭𝐞𝐠𝐨𝐫𝐢𝐞𝐬.


'''

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("ECommerceSetup").getOrCreate()

categories_schema = StructType([
    StructField("CategoryID", IntegerType(), False),
    StructField("CategoryName", StringType(), True)
])

categories_data = [
    (1, "Electronics"),
    (2, "Books"),
    (3, "Clothing")
]

df_categories = spark.createDataFrame(categories_data, categories_schema)

products_schema = StructType([
    StructField("ProductID", IntegerType(), False),
    StructField("ProductName", StringType(), True),
    StructField("CategoryID", IntegerType(), True)
])

products_data = [
    (1, "Laptop", 1),
    (2, "Smartphone", 1),
    (3, "Novel", 2),
    (4, "T-Shirt", 3),
    (5, "Jeans", 3),
    (6, "Textbook", 2)
]

df_products = spark.createDataFrame(products_data, products_schema)

customers_schema = StructType([
    StructField("CustomerID", IntegerType(), False),
    StructField("CustomerName", StringType(), True)
])

customers_data = [
    (101, "Alice"),
    (102, "Bob"),
    (103, "Charlie")
]

df_customers = spark.createDataFrame(customers_data, customers_schema)

purchases_schema = StructType([
    StructField("PurchaseID", IntegerType(), False),
    StructField("CustomerID", IntegerType(), True),
    StructField("ProductID", IntegerType(), True),
    StructField("PurchaseDate", StringType(), True)
])

purchases_data = [
    (1, 101, 1, "2024-01-10"),
    (2, 101, 3, "2024-01-12"),
    (3, 101, 4, "2024-01-15"),
    (4, 102, 1, "2024-02-10"),
    (5, 102, 3, "2024-02-12"),
    (6, 103, 4, "2024-03-10"),
    (7, 103, 6, "2024-03-12"),
    (8, 103, 2, "2024-03-14")
]

df_purchases = spark.createDataFrame(purchases_data, purchases_schema)

num_prods = df_categories.count()

df_purchases.join(df_products,on="ProductID",how="inner").join(df_customers,on="CustomerID",how="inner").\
    groupBy("CustomerID","CustomerName").agg(F.count_distinct("CategoryID").alias("num_categories")).where(F.col("num_categories") == num_prods).\
        select("CustomerID","CustomerName").show()