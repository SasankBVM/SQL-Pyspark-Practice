'''

𝐅𝐢𝐧𝐝 𝐭𝐡𝐞 𝐭𝐨𝐩 𝟐 𝐦𝐨𝐬𝐭 𝐟𝐫𝐞𝐪𝐮𝐞𝐧𝐭 𝐩𝐫𝐨𝐝𝐮𝐜𝐭 𝐜𝐨𝐦𝐛𝐢𝐧𝐚𝐭𝐢𝐨𝐧𝐬 𝐛𝐨𝐮𝐠𝐡𝐭 𝐭𝐨𝐠𝐞𝐭𝐡𝐞𝐫.

'''

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("OrdersDetailsSetup").getOrCreate()

orders_schema = StructType([
    StructField("OrderID", IntegerType(), True),
    StructField("ProductName", StringType(), True)
])

orders_data = [
    (1, "Milk"), (1, "Bread"), (1, "Butter"),
    (2, "Milk"), (2, "Bread"),
    (3, "Milk"), (3, "Butter"),
    (4, "Bread"), (4, "Butter"),
    (5, "Milk"), (5, "Bread"), (5, "Butter"),
    (6, "Milk"), (6, "Bread"),
    (7, "Bread"), (7, "Butter"),
    (8, "Milk"), (8, "Butter")
]

df_orders_details = spark.createDataFrame(orders_data, orders_schema)

df_1 = df_orders_details.withColumnRenamed("OrderID","df1_order_id").withColumnRenamed("ProductName","df1_prod_name")
df_2 = df_orders_details.withColumnRenamed("OrderID","df2_order_id").withColumnRenamed("ProductName","df2_prod_name")

df_1.join(df_2,(df_1["df1_order_id"] == df_2["df2_order_id"]) & (df_1["df1_prod_name"]< df_2["df2_prod_name"]),how="inner")\
    .withColumn("smaller",F.least(F.col("df1_prod_name"),F.col("df2_prod_name"))).withColumn("greater",F.greatest(F.col("df1_prod_name"),F.col("df2_prod_name")))\
.select("smaller","greater").groupBy("smaller","greater").agg(F.count("*").alias("freq")).orderBy(F.col("freq").desc()).limit(2).show()