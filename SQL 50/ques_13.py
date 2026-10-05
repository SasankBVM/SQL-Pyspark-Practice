from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

sales_person_schema = StructType([
    StructField("sales_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("commission_rate", IntegerType(), True),
    StructField("hire_date", StringType(), True)
])

sales_person_data = [
    (1, "John", 100000, 6, "4/1/2006"),
    (2, "Amy", 12000, 5, "5/1/2010"),
    (3, "Mark", 65000, 12, "12/25/2008"),
    (4, "Pam", 25000, 25, "1/1/2005"),
    (5, "Alex", 5000, 10, "2/3/2007")
]

sales_person_df = spark.createDataFrame(sales_person_data, schema=sales_person_schema).alias("a")

company_schema = StructType([
    StructField("com_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True)
])

company_data = [
    (1, "RED", "Boston"),
    (2, "ORANGE", "New York"),
    (3, "YELLOW", "Boston"),
    (4, "GREEN", "Austin")
]

company_df = spark.createDataFrame(company_data, schema=company_schema).alias("b")

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("com_id", IntegerType(), True),
    StructField("sales_id", IntegerType(), True),
    StructField("amount", IntegerType(), True)
])

orders_data = [
    (1, "1/1/2014", 3, 4, 10000),
    (2, "2/1/2014", 4, 5, 5000),
    (3, "3/1/2014", 1, 1, 50000),
    (4, "4/1/2014", 1, 4, 25000)
]

orders_df = spark.createDataFrame(orders_data, schema=orders_schema)

first_df = orders_df.join(company_df, on= "com_id")\
    .filter(F.col("b.name")=="RED")

sales_person_df.join(first_df,on="sales_id",how="left_anti").select("a.name").show()