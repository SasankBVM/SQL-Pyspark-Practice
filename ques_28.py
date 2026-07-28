'''

𝐑𝐚𝐧𝐤 𝐬𝐭𝐨𝐫𝐞𝐬 𝐛𝐲 𝐭𝐡𝐞𝐢𝐫 𝐦𝐨𝐧𝐭𝐡𝐥𝐲 𝐬𝐚𝐥𝐞𝐬 𝐩𝐞𝐫𝐟𝐨𝐫𝐦𝐚𝐧𝐜𝐞.


'''
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("StoreSalesSetup").getOrCreate()

store_sales_schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("store_name", StringType(), True),
    StructField("sale_date", StringType(), True),
    StructField("sales_amount", FloatType(), True)
])

store_sales_data = [
    (1, 101, "Store A", "2024-01-15", 10000.00),
    (2, 102, "Store B", "2024-01-20", 15000.00),
    (3, 103, "Store C", "2024-01-25", 12000.00),
    (4, 101, "Store A", "2024-02-10", 14000.00),
    (5, 102, "Store B", "2024-02-18", 13000.00),
    (6, 103, "Store C", "2024-02-25", 16000.00),
    (7, 101, "Store A", "2024-03-05", 11000.00),
    (8, 102, "Store B", "2024-03-15", 9000.00),
    (9, 103, "Store C", "2024-03-20", 12500.00)
]

df_store_sales = spark.createDataFrame(store_sales_data, store_sales_schema)

window_spec = Window.partitionBy("month","store_id")
window_spec1 = Window.partitionBy("month").orderBy(F.col("month").asc(),F.col("total_amt").desc())

df_store_sales.withColumn("month",F.month("sale_date")).withColumn("total_amt",F.sum("sales_amount").over(window_spec)).\
    withColumn("rnk",F.row_number().over(window_spec1)).select("month","store_id","store_name","total_amt","rnk").show()
