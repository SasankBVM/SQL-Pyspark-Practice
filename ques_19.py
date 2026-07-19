'''

𝐃𝐞𝐭𝐞𝐜𝐭 𝐚𝐧𝐨𝐦𝐚𝐥𝐢𝐞𝐬 𝐰𝐡𝐞𝐫𝐞 𝐬𝐚𝐥𝐞𝐬 𝐟𝐨𝐫 𝐚 𝐩𝐫𝐨𝐝𝐮𝐜𝐭 𝐚𝐫𝐞 𝟓𝟎% 𝐥𝐨𝐰𝐞𝐫 𝐭𝐡𝐚𝐧 𝐭𝐡𝐞 𝐚𝐯𝐞𝐫𝐚𝐠𝐞 𝐟𝐨𝐫
𝐭𝐡𝐚𝐭 𝐩𝐫𝐨𝐝𝐮𝐜𝐭


'''
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
import pyspark.sql.functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("ProductsSalesSetup").getOrCreate()

products_schema = StructType([
    StructField("ProductID", IntegerType(), False),
    StructField("ProductName", StringType(), True)
])

products_data = [
    (1, "Smartphone"),
    (2, "Laptop"),
    (3, "Tablet")
]

df_products = spark.createDataFrame(products_data, products_schema)

sales_schema = StructType([
    StructField("SaleID", IntegerType(), False),
    StructField("ProductID", IntegerType(), True),
    StructField("SaleDate", StringType(), True),
    StructField("SaleAmount", FloatType(), True)
])

sales_data = [
    (1, 1, "2025-06-01", 21000.00),
    (2, 1, "2025-06-05", 20000.00),
    (3, 1, "2025-06-10", 19000.00),
    (4, 1, "2025-06-15", 9000.00),
    (5, 2, "2025-06-01", 50000.00),
    (6, 2, "2025-06-07", 45000.00),
    (7, 2, "2025-06-20", 34000.00),
    (8, 2, "2025-06-25", 20000.00),
    (9, 3, "2025-06-03", 15000.00),
    (10, 3, "2025-06-08", 15500.00),
    (11, 3, "2025-06-13", 14500.00),
    (12, 3, "2025-06-18", 6000.00)
]

df_sales = spark.createDataFrame(sales_data, sales_schema)

window_spec = Window.partitionBy("ProductID")

df_avgs = df_sales.withColumn("avg_sales",F.avg("SaleAmount").over(window_spec))

df_avgs.join(df_products,on="ProductID",how="inner").filter(F.col("SaleAmount") < F.lit(0.5)*F.col("avg_sales")).\
    select("ProductID","ProductName","SaleAmount","avg_sales").show()