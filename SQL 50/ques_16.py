from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder \
    .appName("ques_16") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

project_schema = StructType([
    StructField("project_id", IntegerType(), True),
    StructField("employee_id", IntegerType(), True)
])

project_data = [
    (1, 1),
    (1, 2),
    (1, 3),
    (2, 1),
    (2, 4)
]

project_df = spark.createDataFrame(project_data, schema=project_schema)

employee_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("experience_years", IntegerType(), True)
])

employee_data = [
    (1, "Khaled", 3),
    (2, "Ali", 2),
    (3, "John", 3),
    (4, "Doe", 2)
]

employee_df = spark.createDataFrame(employee_data, schema=employee_schema)

db_properties = {
    "user": "postgres",               
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}
df_jdbc = spark.read.jdbc(url="jdbc:postgresql://localhost:5432/cricket_analyzer",table="batsmen_odi",properties=db_properties)
df_jdbc.show()

# project_df.join(employee_df, on="employee_id",how="inner").\
#     withColumn("rnk",F.dense_rank().over(Window.partitionBy("project_id").orderBy(F.col("experience_years").desc()))).\
#         filter(F.col("rnk") == 1).select("project_id","employee_id","experience_years").show()