'''
𝐅𝐢𝐧𝐝 𝐭𝐡𝐞 𝐭𝐨𝐩 𝟐 𝐡𝐢𝐠𝐡𝐞𝐬𝐭-𝐬𝐞𝐥𝐥𝐢𝐧𝐠 𝐩𝐫𝐨𝐝𝐮𝐜𝐭𝐬 𝐟𝐨𝐫 𝐞𝐚𝐜𝐡 𝐜𝐚𝐭𝐞𝐠𝐨𝐫𝐲.

'''


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
import pyspark.sql.functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("CategoriesProductsSalesSetup").getOrCreate()

categories_schema = StructType([
    StructField("CategoryID", IntegerType(), False),
    StructField("CategoryName", StringType(), True)
])

categories_data = [
    (1, "Electronics"),
    (2, "Clothing"),
    (3, "Groceries")
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
    (3, "Headphones", 1),
    (4, "T-shirt", 2),
    (5, "Jeans", 2),
    (6, "Jacket", 2),
    (7, "Rice", 3),
    (8, "Wheat Flour", 3),
    (9, "Sugar", 3)
]

df_products = spark.createDataFrame(products_data, products_schema)

sales_schema = StructType([
    StructField("SaleID", IntegerType(), False),
    StructField("ProductID", IntegerType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Price", FloatType(), True)
])

sales_data = [
    (1, 1, 5, 50000.00),
    (2, 2, 10, 30000.00),
    (3, 3, 15, 2000.00),
    (4, 4, 20, 500.00),
    (5, 5, 15, 1500.00),
    (6, 6, 10, 3000.00),
    (7, 7, 50, 80.00),
    (8, 8, 60, 50.00),
    (9, 9, 40, 60.00)
]

window_spec = Window.partitionBy("CategoryName").orderBy(F.col("Price").desc())

df_sales = spark.createDataFrame(sales_data, sales_schema)

df_categories.join(df_products,on="CategoryID",how="inner").join(df_sales,on="ProductID",how="inner").\
    withColumn("rank",F.row_number().over(window_spec)).where(F.col("rank")<=2).\
        withColumnRenamed("Price","total_amount").\
        select("CategoryID","CategoryName","ProductID","ProductName","total_amount").\
        orderBy(F.col("CategoryID")).show()
    