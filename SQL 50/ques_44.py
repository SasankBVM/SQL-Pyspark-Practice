from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

daily_sales_schema = StructType([
    StructField("date_id", StringType(), True),
    StructField("make_name", StringType(), True),
    StructField("lead_id", IntegerType(), True),
    StructField("partner_id", IntegerType(), True)
])

daily_sales_data = [
    ("2020-12-8", "toyota", 0, 1),
    ("2020-12-8", "toyota", 1, 0),
    ("2020-12-8", "toyota", 1, 2),
    ("2020-12-7", "toyota", 0, 2),
    ("2020-12-7", "toyota", 0, 1),
    ("2020-12-8", "honda", 1, 2),
    ("2020-12-8", "honda", 2, 1),
    ("2020-12-7", "honda", 0, 1),
    ("2020-12-7", "honda", 1, 2),
    ("2020-12-7", "honda", 2, 1)
]

daily_sales_df = spark.createDataFrame(daily_sales_data, schema=daily_sales_schema)

daily_sales_df.groupBy("date_id","make_name").agg(F.count_distinct(F.col("lead_id")).alias("unique_leads"), F.count_distinct(F.col("partner_id")).alias("unique_partners")).show()