'''
𝐅𝐢𝐧𝐝 𝐜𝐮𝐬𝐭𝐨𝐦𝐞𝐫𝐬 𝐰𝐡𝐨 𝐩𝐥𝐚𝐜𝐞𝐝 𝐦𝐨𝐫𝐞 𝐭𝐡𝐚𝐧 𝟓𝟎% 𝐨𝐟 𝐭𝐡𝐞𝐢𝐫 𝐨𝐫𝐝𝐞𝐫𝐬 𝐢𝐧 𝐭𝐡𝐞 𝐥𝐚𝐬𝐭 𝐦𝐨𝐧𝐭𝐡.
'''


import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,DateType
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("CustomersOrdersSetup").getOrCreate()

customers_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_name", StringType(), True)
])

customers_data = [
    (1, "Alice"),
    (2, "Bob"),
    (3, "Charlie"),
    (4, "David")
]

df_customers_data = spark.createDataFrame(customers_data, customers_schema)

orders_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", StringType(), True)
])

orders_data = [
    (101, 1, "2025-06-05"),
    (102, 1, "2025-05-10"),
    (103, 1, "2025-06-15"),
    (104, 1, "2025-04-20"),
    (105, 2, "2025-06-02"),
    (106, 2, "2025-06-08"),
    (107, 3, "2025-04-01"),
    (108, 3, "2025-05-03"),
    (109, 3, "2025-06-25"),
    (110, 4, "2025-05-18"),
    (111, 4, "2025-05-25")
]

df_orders_data = spark.createDataFrame(orders_data, orders_schema)

max_date = df_orders_data.select(F.max("order_date").alias("max_date"))

df_orders_data.crossJoin(max_date).groupBy("customer_id","max_date").agg(F.count("order_id").alias("total_orders"),F.count(F.when(F.month("order_date") == F.month("max_date")-1,1)).alias("last_month_orders")).\
    select("customer_id","max_date","total_orders","last_month_orders").where(F.col("total_orders").cast(IntegerType())/2 < F.col("last_month_orders")).\
        join(F.broadcast(df_customers_data),on="customer_id",how="inner").select(F.monthname(F.add_months("max_date",-1)).alias("Order_month"),"customer_id","customer_name").show()