'''

𝐂𝐚𝐥𝐜𝐮𝐥𝐚𝐭𝐞 𝐭𝐡𝐞 𝐜𝐮𝐦𝐮𝐥𝐚𝐭𝐢𝐯𝐞 𝐬𝐚𝐥𝐞𝐬 𝐟𝐨𝐫 𝐞𝐚𝐜𝐡 𝐬𝐭𝐨𝐫𝐞, 𝐛𝐮𝐭 𝐨𝐧𝐥𝐲 
𝐢𝐧𝐜𝐥𝐮𝐝𝐞 𝐝𝐚𝐭𝐞𝐬 𝐰𝐡𝐞𝐫𝐞 𝐭𝐡𝐞 𝐝𝐚𝐢𝐥𝐲 𝐬𝐚𝐥𝐞𝐬 𝐞𝐱𝐜𝐞𝐞𝐝𝐞𝐝 𝐭𝐡𝐞 𝐬𝐭𝐨𝐫𝐞'𝐬 𝐚𝐯𝐞𝐫𝐚𝐠𝐞 𝐝𝐚𝐢𝐥𝐲 𝐬𝐚𝐥𝐞𝐬.


'''



import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("SalesDataSetup").getOrCreate()

sales_schema = StructType([
    StructField("Store_ID", IntegerType(), True),
    StructField("Sale_Date", StringType(), True),
    StructField("Daily_Sales", IntegerType(), True)
])

sales_data = [
    (1, "2024-06-01", 1000),
    (1, "2024-06-02", 1200),
    (1, "2024-06-03", 800),
    (1, "2024-06-04", 1500),
    (2, "2024-06-01", 500),
    (2, "2024-06-02", 700),
    (2, "2024-06-03", 900),
    (2, "2024-06-04", 400)
]

df_sales_data = spark.createDataFrame(sales_data, sales_schema)

window_spec = Window.partitionBy("Store_ID")
window_spec1 = Window.partitionBy("Store_ID").orderBy("Sale_Date")\
.rowsBetween(Window.unboundedPreceding, Window.currentRow) # To handle the edge case where two records having same date and same store_id, else it will sum up both

df_sales_data.withColumn("average_sales",F.avg("Daily_Sales").over(window_spec)).filter(F.col("Daily_Sales")>F.col("average_sales")).\
    withColumn("cummu_sum",F.sum("Daily_Sales").over(window_spec1)).select("Store_ID","Sale_Date","Daily_Sales","cummu_sum").show()