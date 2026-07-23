'''

𝐈𝐝𝐞𝐧𝐭𝐢𝐟𝐲 𝐬𝐮𝐩𝐩𝐥𝐢𝐞𝐫𝐬 𝐰𝐡𝐨𝐬𝐞 𝐚𝐯𝐞𝐫𝐚𝐠𝐞 𝐝𝐞𝐥𝐢𝐯𝐞𝐫𝐲 𝐭𝐢𝐦𝐞 𝐢𝐬 𝐥𝐞𝐬𝐬
𝐭𝐡𝐚𝐧 𝟐 𝐝𝐚𝐲𝐬, 𝐛𝐮𝐭 𝐨𝐧𝐥𝐲 𝐜𝐨𝐧𝐬𝐢𝐝𝐞𝐫 𝐝𝐞𝐥𝐢𝐯𝐞𝐫𝐢𝐞𝐬 𝐰𝐢𝐭𝐡 𝐪𝐮𝐚𝐧𝐭𝐢𝐭𝐢𝐞𝐬 𝐠𝐫𝐞𝐚𝐭𝐞𝐫 𝐭𝐡𝐚𝐧 𝟏𝟎𝟎 𝐮𝐧𝐢𝐭𝐬.


'''

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("SuppliersDeliveriesSetup").getOrCreate()

suppliers_schema = StructType([
    StructField("SupplierID", IntegerType(), False),
    StructField("SupplierName", StringType(), True)
])

suppliers_data = [
    (1, "Alpha Supplies"),
    (2, "Beta Traders"),
    (3, "Gamma Distributors"),
    (4, "Delta Logistics")
]

df_suppliers_data = spark.createDataFrame(suppliers_data, suppliers_schema)

deliveries_schema = StructType([
    StructField("DeliveryID", IntegerType(), False),
    StructField("SupplierID", IntegerType(), True),
    StructField("OrderDate", StringType(), True),
    StructField("DeliveryDate", StringType(), True),
    StructField("Quantity", IntegerType(), True)
])

deliveries_data = [
    (101, 1, "2024-06-01", "2024-06-02", 120),
    (102, 1, "2024-06-03", "2024-06-05", 80),
    (103, 1, "2024-06-05", "2024-06-06", 150),
    (104, 2, "2024-06-02", "2024-06-05", 200),
    (105, 2, "2024-06-04", "2024-06-06", 110),
    (106, 3, "2024-06-03", "2024-06-04", 130),
    (107, 3, "2024-06-06", "2024-06-08", 140),
    (108, 4, "2024-06-07", "2024-06-08", 90),
    (109, 4, "2024-06-09", "2024-06-12", 160)
]

df_deliveries = spark.createDataFrame(deliveries_data, deliveries_schema)

df_deliveries.withColumn("delivery_days",F.date_diff(F.col("DeliveryDate"),F.col("OrderDate")))\
    .where(F.col("Quantity") > 100)\
    .groupBy(F.col("SupplierID")).agg(F.avg("delivery_days").alias("average_delivery_days"))\
    .where(F.col("average_delivery_days")<2)\
    .select("SupplierID").join(F.broadcast(df_suppliers_data),on="SupplierID",how="inner")\
    .show()