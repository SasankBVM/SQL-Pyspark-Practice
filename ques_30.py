'''
To display the records with three or more rows with consecutive id's, and the number of people is greater than or equal to 100 for each
'''

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("VisitsSetup").getOrCreate()

visits_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("visit_date", StringType(), True),
    StructField("people", IntegerType(), True)
])

visits_data = [
    (1, "2017-01-01", 10),
    (2, "2017-01-02", 109),
    (3, "2017-01-03", 150),
    (4, "2017-01-04", 99),
    (5, "2017-01-05", 145),
    (6, "2017-01-06", 1455),
    (7, "2017-01-07", 199),
    (8, "2017-01-09", 188)
]

window_spec = Window.partitionBy().orderBy("id")
df_visits = spark.createDataFrame(visits_data, visits_schema)

df_visits.filter(F.col("people")>=100).withColumn("rn",F.row_number().over(window_spec)).withColumn("grouped_id",F.col("id")-F.col("rn")).\
    withColumn("count_groups",F.count("grouped_id").over(Window.partitionBy("grouped_id"))).filter(F.col("count_groups")>=3).select("id","visit_date","people").show()
