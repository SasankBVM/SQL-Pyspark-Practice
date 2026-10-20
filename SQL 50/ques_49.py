from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType

spark = SparkSession.builder.getOrCreate()

tasks_schema = StructType([
    StructField("task_id", IntegerType(), True),
    StructField("subtasks_count", IntegerType(), True)
])

tasks_data = [
    (1, 3),
    (2, 2),
    (3, 4)
]

tasks_df = spark.createDataFrame(tasks_data, schema=tasks_schema)

executed_schema = StructType([
    StructField("task_id", IntegerType(), True),
    StructField("subtask_id", IntegerType(), True)
])

executed_data = [
    (1, 2),
    (3, 1),
    (3, 2),
    (3, 3),
    (3, 4)
]

executed_df = spark.createDataFrame(executed_data, schema=executed_schema)
tasks_df.createTempView("tdf")

df_all = spark.sql(""" 
    
    with recursive cte as (
        select task_id, subtasks_count as subtask_id from tdf
        union all
        select task_id, subtask_id-1
        from cte
        where subtask_id > 1
    )
    select * from cte
          """)

df_all.alias("a").join(executed_df.alias("b"),on= (F.col("a.task_id") == F.col("b.task_id")) & (F.col("a.subtask_id") == F.col("b.subtask_id")),how="left").filter(F.col("b.subtask_id").isNull()).\
    select(F.col("a.task_id").alias("task_id"),F.col("a.subtask_id").alias("subtask_id")).orderBy("task_id","subtask_id").show()