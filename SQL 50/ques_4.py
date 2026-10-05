from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("article_id", IntegerType(), True),
    StructField("author_id", IntegerType(), True),
    StructField("viewer_id", IntegerType(), True),
    StructField("view_date", StringType(), True)
])

data = [
    (1, 3, 5, "2019-08-01"),
    (1, 3, 6, "2019-08-02"),
    (2, 7, 7, "2019-08-01"),
    (2, 7, 6, "2019-08-02"),
    (4, 7, 1, "2019-07-22"),
    (3, 4, 4, "2019-07-21"),
    (3, 4, 4, "2019-07-21")
]

df = spark.createDataFrame(data, schema=schema)

df.filter(F.col("author_id") == F.col("viewer_id")).select(F.col("author_id")).distinct().orderBy("author_id").show()