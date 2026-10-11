from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("sale_date", StringType(), True),
    StructField("fruit", StringType(), True),
    StructField("sold_num", IntegerType(), True)
])

data = [
    ("2020-05-01", "apples", 10),
    ("2020-05-01", "oranges", 8),
    ("2020-05-02", "apples", 15),
    ("2020-05-02", "oranges", 15),
    ("2020-05-03", "apples", 20),
    ("2020-05-03", "oranges", 0),
    ("2020-05-04", "apples", 15),
    ("2020-05-04", "oranges", 16)
]

df = spark.createDataFrame(data, schema=schema)

window_spec = Window.partitionBy("sale_date").orderBy("sale_date")

df.groupBy("sale_date").agg(
                                    F.sum(F.when(F.col("fruit") == "apples", F.lit(F.col("sold_num")))).alias("apple_count"), 
                                    F.sum(F.when(F.col("fruit") == "oranges", F.lit(F.col("sold_num")))).alias("orange_count")
                                    ).withColumn("diff", F.col("apple_count")-F.col("orange_count")).select("sale_date","diff").\
                                    orderBy(F.col("sale_date")).show()