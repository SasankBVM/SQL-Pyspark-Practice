'''
𝐌𝐚𝐬𝐭𝐞𝐫𝐜𝐚𝐫𝐝, 𝐛𝐞𝐢𝐧𝐠 𝐚 𝐠𝐥𝐨𝐛𝐚𝐥 𝐭𝐞𝐜𝐡𝐧𝐨𝐥𝐨𝐠𝐲 𝐜𝐨𝐦𝐩𝐚𝐧𝐲 𝐢𝐧 𝐭𝐡𝐞 𝐩𝐚𝐲𝐦𝐞𝐧𝐭𝐬 𝐢𝐧𝐝𝐮𝐬𝐭𝐫𝐲,
𝐥𝐢𝐤𝐞𝐥𝐲 𝐡𝐚𝐬 𝐯𝐚𝐬𝐭 𝐚𝐦𝐨𝐮𝐧𝐭𝐬 𝐨𝐟 𝐭𝐫𝐚𝐧𝐬𝐚𝐜𝐭𝐢𝐨𝐧 𝐝𝐚𝐭𝐚. 𝐇𝐞𝐫𝐞 𝐢𝐬 𝐚 𝐡𝐲𝐩𝐨𝐭𝐡𝐞𝐭𝐢𝐜𝐚𝐥 𝐬𝐢𝐭𝐮𝐚𝐭𝐢𝐨𝐧
𝐰𝐡𝐞𝐫𝐞 𝐲𝐨𝐮 𝐚𝐫𝐞 𝐚𝐬𝐤𝐞𝐝 𝐭𝐨 𝐚𝐧𝐚𝐥𝐲𝐳𝐞 𝐬𝐮𝐜𝐡 𝐝𝐚𝐭𝐚:

𝐆𝐢𝐯𝐞𝐧 𝐚 𝐭𝐚𝐛𝐥𝐞 "𝐭𝐫𝐚𝐧𝐬𝐚𝐜𝐭𝐢𝐨𝐧𝐬" 𝐰𝐡𝐞𝐫𝐞 𝐞𝐚𝐜𝐡 𝐫𝐨𝐰 𝐫𝐞𝐩𝐫𝐞𝐬𝐞𝐧𝐭𝐬 𝐚 𝐭𝐫𝐚𝐧𝐬𝐚𝐜𝐭𝐢𝐨𝐧 𝐦𝐚𝐝𝐞
𝐛𝐲 𝐚 𝐜𝐮𝐬𝐭𝐨𝐦𝐞𝐫, 𝐲𝐨𝐮𝐫 𝐭𝐚𝐬𝐤 𝐢𝐬 𝐭𝐨 𝐰𝐫𝐢𝐭𝐞 𝐒𝐐𝐋 𝐪𝐮𝐞𝐫𝐲 𝐭𝐡𝐚𝐭 𝐰𝐢𝐥𝐥 𝐟𝐢𝐧𝐝 𝐭𝐡𝐞 𝐭𝐨𝐭𝐚𝐥 𝐧𝐮𝐦𝐛𝐞𝐫 
𝐨𝐟 𝐭𝐫𝐚𝐧𝐬𝐚𝐜𝐭𝐢𝐨𝐧𝐬 𝐚𝐧𝐝 𝐭𝐨𝐭𝐚𝐥 𝐚𝐦𝐨𝐮𝐧𝐭 𝐬𝐩𝐞𝐧𝐭 𝐩𝐞𝐫 𝐜𝐮𝐬𝐭𝐨𝐦𝐞𝐫 𝐟𝐨𝐫 𝐞𝐚𝐜𝐡 𝐪𝐮𝐚𝐫𝐭𝐞𝐫 𝐨𝐟 𝟐𝟎𝟐𝟏. 
𝐀𝐝𝐝𝐢𝐭𝐢𝐨𝐧𝐚𝐥𝐥𝐲, 𝐝𝐞𝐫𝐢𝐯𝐞 𝐭𝐡𝐞 𝐪𝐮𝐚𝐫𝐭𝐞𝐫 𝐨𝐯𝐞𝐫 𝐪𝐮𝐚𝐫𝐭𝐞𝐫 𝐠𝐫𝐨𝐰𝐭𝐡 𝐢𝐧 𝐭𝐨𝐭𝐚𝐥 𝐚𝐦𝐨𝐮𝐧𝐭 𝐬𝐩𝐞𝐧𝐭 𝐟𝐨𝐫 𝐞𝐚𝐜𝐡 𝐮𝐬𝐞𝐫.
*/

'''


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import col,quarter,year,count,sum,concat,lit,lag,coalesce,round,try_divide
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("TransactionsData").getOrCreate()

transactions_schema = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("purchase_date", StringType(), True),
    StructField("amount", FloatType(), True)
])

transactions_data = [
    (9153, 568, "2021-03-15", 120.00),
    (1478, 859, "2021-04-24", 200.00),
    (3491, 568, "2021-06-20", 180.00),
    (2608, 859, "2021-07-08", 220.00),
    (7083, 437, "2021-09-27", 80.00),
    (3297, 568, "2021-12-18", 100.00)
]

windowSpec = Window.partitionBy(col("customer_id")).orderBy(col("Quarter"))

df_transactions = spark.createDataFrame(transactions_data, transactions_schema)

df_transactions.filter(year(col("purchase_date")) == 2021).withColumn("Quarter",concat(lit("Q"),quarter(col("purchase_date")),lit("-2021"))).\
groupBy(col("customer_id"),col("Quarter")).\
agg(count(col("transaction_id")).alias("transaction_count"),sum(col("amount")).alias("total_spent")).\
withColumn("prev_quarter_spent",coalesce(lag(col("total_spent")).over(windowSpec),lit(0.0))).\
withColumn("quarter_over_quarter_growth",round(
    coalesce(try_divide(((col("total_spent")-col("prev_quarter_spent"))*100),col("prev_quarter_spent")),lit(0.0)),
2).alias("quarter_over_quarter_growth")).\
select("Quarter","customer_id","transaction_count","total_spent","prev_quarter_spent","quarter_over_quarter_growth").\
orderBy("customer_id","Quarter").show()