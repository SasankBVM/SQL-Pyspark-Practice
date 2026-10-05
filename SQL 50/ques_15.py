from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("x", IntegerType(), True)
])

data = [
    (-1,),
    (0,),
    (2,)
]

df = spark.createDataFrame(data, schema=schema).alias("a")
df1 = df.alias("b")

df.join(df1,how="cross").filter(F.col("a.x")!=F.col("b.x")).withColumn("dis",F.abs(F.col("a.x")-F.col("b.x"))).orderBy("dis").limit(1).show()