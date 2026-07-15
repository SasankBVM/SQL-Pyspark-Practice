from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("TransactionRecordsSetup").getOrCreate()

transaction_records_schema = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("user_id", StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("amount_usd", FloatType(), True)
])

transaction_records_data = [
    (1234, "_MC_GOLD_001", "2022-06-05 10:30:00", 600.00),
    (5678, "_MC_GOLD_002", "2022-06-15 16:30:00", 320.00),
    (2467, "MC_PLATINUM_003", "2022-06-20 11:00:00", 780.00),
    (2355, "_MC_GOLD_001", "2022-06-25 14:30:00", 680.00),
    (7654, "MC_PLATINUM_004", "2022-06-30 09:45:00", 520.00)
]

df_transaction_records = spark.createDataFrame(transaction_records_data, transaction_records_schema)

df_transaction_records.filter(
                            (F.col("user_id").startswith("_MC_GOLD")) &
                            (F.month(F.col("transaction_date")) == 6) &
                            (F.col("amount_usd") > 500.0)
                            ).groupBy(F.col("user_id")).agg(F.sum(F.col("amount_usd")).alias("Total_amount")).show()