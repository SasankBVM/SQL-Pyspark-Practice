from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

employees_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("name", StringType(), True)
])

employees_data = [
    (2, "Crew"),
    (4, "Haven"),
    (5, "Kristian")
]

employees_df = spark.createDataFrame(employees_data, schema=employees_schema)

salaries_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("salary", IntegerType(), True)
])

salaries_data = [
    (5, 76071),
    (1, 22517),
    (4, 63539)
]

salaries_df = spark.createDataFrame(salaries_data, schema=salaries_schema)

employees_df.alias("e").join(salaries_df.alias("s"),on="employee_id",how="left").filter(F.col("s.salary").isNull()).union(
    employees_df.alias("e").join(salaries_df.alias("s"),on="employee_id",how="right").filter(F.col("name").isNull())
).select("employee_id").orderBy("employee_id").show()