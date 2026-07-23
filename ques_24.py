'''

𝐈𝐝𝐞𝐧𝐭𝐢𝐟𝐲 𝐩𝐫𝐨𝐝𝐮𝐜𝐭𝐬 𝐭𝐡𝐚𝐭 𝐡𝐚𝐯𝐞 𝐛𝐞𝐞𝐧 𝐬𝐨𝐥𝐝 𝐛𝐮𝐭 𝐡𝐚𝐯𝐞 𝐧𝐨 𝐫𝐞𝐜𝐨𝐫𝐝 𝐢𝐧 𝐭𝐡𝐞 𝐩𝐫𝐨𝐝𝐮𝐜𝐭𝐬 𝐭𝐚𝐛𝐥𝐞
𝐚𝐧𝐝 𝐚𝐥𝐬𝐨 𝐜𝐚𝐥𝐜𝐮𝐥𝐚𝐭𝐞 𝐡𝐨𝐰 𝐦𝐚𝐧𝐲 𝐭𝐢𝐦𝐞𝐬 𝐞𝐚𝐜𝐡 𝐦𝐢𝐬𝐬𝐢𝐧𝐠 𝐩𝐫𝐨𝐝𝐮𝐜𝐭 𝐡𝐚𝐬 𝐛𝐞𝐞𝐧 𝐬𝐨𝐥𝐝.


'''


import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

spark = SparkSession.builder.appName("ProductsSalesDetailsSetup").getOrCreate()

products_schema = StructType([
    StructField("ProductID", IntegerType(), False),
    StructField("ProductName", StringType(), True)
])

products_data = [
    (1, "Smartphone"),
    (2, "Laptop"),
    (3, "Tablet")
]

df_products_details = spark.createDataFrame(products_data, products_schema)

sales_schema = StructType([
    StructField("SaleID", IntegerType(), False),
    StructField("ProductID", IntegerType(), True),
    StructField("SaleDate", StringType(), True),
    StructField("SaleAmount", FloatType(), True)
])

sales_data = [
    (101, 1, "2024-06-01", 15000.00),
    (102, 4, "2024-06-02", 20000.00),
    (103, 5, "2024-06-03", 30000.00),
    (104, 4, "2024-06-04", 20000.00),
    (105, 1, "2024-06-05", 15000.00),
    (106, 5, "2024-06-06", 30000.00),
    (107, 2, "2024-06-07", 25000.00)
]

df_sales_details = spark.createDataFrame(sales_data, sales_schema)

df_sales_details.groupBy("ProductID").agg(F.count("ProductID").alias("Missing_Product_Sales_Count")).\
    join(df_products_details, on="ProductID",how="leftanti").show()