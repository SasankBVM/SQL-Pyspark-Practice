from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

employees_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", IntegerType(), True)
])

employees_data = [
    (2, "Meir", 3000),
    (3, "Michael", 3800),
    (7, "Addilyn", 7400),
    (8, "Juan", 6100),
    (9, "Kannon", 7700)
]

employees_df = spark.createDataFrame(employees_data, schema=employees_schema)
employees_df.withColumn("bonus",F.when(((F.col("employee_id")%2 == 1) & ~(F.col("name").like("M%"))),F.lit(F.col("salary"))).otherwise(F.lit(0))).\
    select("employee_id","bonus").orderBy("employee_id").show()