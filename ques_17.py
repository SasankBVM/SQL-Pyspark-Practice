'''

𝐈𝐝𝐞𝐧𝐭𝐢𝐟𝐲 𝐜𝐮𝐬𝐭𝐨𝐦𝐞𝐫𝐬 𝐰𝐡𝐨 𝐦𝐚𝐝𝐞 𝐩𝐮𝐫𝐜𝐡𝐚𝐬𝐞𝐬 𝐨𝐧 𝐞𝐱𝐚𝐜𝐭𝐥𝐲 𝐭𝐡𝐫𝐞𝐞
𝐝𝐢𝐟𝐟𝐞𝐫𝐞𝐧𝐭 𝐝𝐚𝐲𝐬 𝐢𝐧 𝐭𝐡𝐞 𝐥𝐚𝐬𝐭 𝐦𝐨𝐧𝐭𝐡.

'''



from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
import pyspark.sql.functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("PurchasesSetup").getOrCreate()

purchases_schema = StructType([
    StructField("PurchaseID", IntegerType(), False),
    StructField("CustomerID", IntegerType(), True),
    StructField("PurchaseDate", StringType(), True),
    StructField("Amount", FloatType(), True)
])

purchases_data = [
    (1, 101, "2025-05-01", 200.00),
    (2, 101, "2025-05-02", 150.00),
    (3, 101, "2025-05-03", 100.00),
    (4, 102, "2025-05-01", 300.00),
    (5, 102, "2025-05-01", 50.00),
    (6, 102, "2025-05-03", 500.00),
    (7, 102, "2025-05-05", 250.00),
    (8, 103, "2025-05-01", 120.00),
    (9, 103, "2025-05-02", 80.00),
    (10, 104, "2025-05-01", 300.00),
    (11, 104, "2025-05-02", 200.00),
    (12, 104, "2025-05-03", 100.00),
    (13, 104, "2025-05-04", 100.00),
    (14, 105, "2025-05-10", 180.00),
    (15, 105, "2025-05-15", 220.00),
    (16, 105, "2025-05-20", 150.00),
    (17, 106, "2025-04-28", 300.00),
    (18, 106, "2025-06-01", 200.00)
]

window_spec = Window.partitionBy(F.col("CustomerID")).orderBy(F.col("PurchaseDate"))

df_purchases = spark.createDataFrame(purchases_data, purchases_schema)

max_date = df_purchases.select(F.max(F.col("PurchaseDate"))).collect()[0][0]

df_purchases \
    .filter(F.month(F.col("PurchaseDate")) == F.month(F.add_months(F.lit(max_date), -1))) \
    .groupBy("CustomerID") \
    .agg(F.countDistinct("PurchaseDate").alias("unique_days")) \
    .where(F.col("unique_days") == 3) \
    .select("CustomerID") \
    .show()    