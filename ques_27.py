import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("ProductSalesSetup").getOrCreate()

product_sales_schema = StructType([
    StructField("sale_id", IntegerType(), False),
    StructField("product_id", StringType(), True),
    StructField("sale_date", StringType(), True),
    StructField("sales_amount", IntegerType(), True)
])

product_sales_data = [
    (1, "P001", "2024-01-01", 100),
    (2, "P001", "2024-01-03", 150),
    (3, "P001", "2024-01-05", 200),
    (4, "P001", "2024-01-06", 180),
    (5, "P001", "2024-01-07", 170),
    (6, "P001", "2024-01-08", 190),
    (7, "P001", "2024-01-09", 210),
    (8, "P001", "2024-01-10", 220),
    (9, "P002", "2024-01-01", 120),
    (10, "P002", "2024-01-04", 130),
    (11, "P002", "2024-01-06", 160),
    (12, "P002", "2024-01-09", 140),
    (13, "P002", "2024-01-10", 150)
]

window_spec = Window.partitionBy("product_id").orderBy("sale_date").rowsBetween(-6,Window.currentRow)

df_product_sales = spark.createDataFrame(product_sales_data, product_sales_schema)

df_product_sales.withColumn("avg_sales_last_7_entries",F.avg("sales_amount").over(window_spec).cast(IntegerType())).select("sale_id","product_id","avg_sales_last_7_entries").show()