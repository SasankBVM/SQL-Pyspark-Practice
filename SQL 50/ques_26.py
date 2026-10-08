from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("employee_name", StringType(), True),
    StructField("manager_id", IntegerType(), True)
])

data = [
    (1, "Boss", 1),
    (3, "Alice", 3),
    (2, "Bob", 1),
    (4, "Daniel", 2),
    (7, "Luis", 4),
    (8, "Jhon", 3),
    (9, "Angela", 8),
    (77, "Robert", 1)
]

df = spark.createDataFrame(data, schema=schema)
df.createTempView("my_df")

recursive_cte = """
WITH RECURSIVE cte as (
    
    SELECT employee_id
    FROM my_df
    where employee_id = manager_id
    
    union all
    
    select b.employee_id
    from cte a
    inner join 
    my_df b
    on a.employee_id = b.manager_id
    where b.employee_id != b.manager_id
)
select *
from cte
"""
df = spark.sql(recursive_cte)
df.show()