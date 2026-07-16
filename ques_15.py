'''
𝐌𝐚𝐬𝐭𝐞𝐫𝐜𝐚𝐫𝐝 𝐡𝐚𝐬 𝐭𝐰𝐨 𝐭𝐚𝐛𝐥𝐞𝐬, 𝐨𝐧𝐞 𝐢𝐬 𝐭𝐡𝐞 𝐜𝐮𝐬𝐭𝐨𝐦𝐞𝐫 𝐭𝐚𝐛𝐥𝐞 𝐭𝐡𝐚𝐭 𝐜𝐨𝐧𝐭𝐚𝐢𝐧𝐬 𝐢𝐧𝐟𝐨𝐫𝐦𝐚𝐭𝐢𝐨𝐧 
𝐚𝐛𝐨𝐮𝐭 𝐭𝐡𝐞 𝐜𝐮𝐬𝐭𝐨𝐦𝐞𝐫𝐬, 𝐚𝐧𝐝 𝐭𝐡𝐞 𝐨𝐭𝐡𝐞𝐫 𝐢𝐬 𝐭𝐡𝐞 𝐭𝐫𝐚𝐧𝐬𝐚𝐜𝐭𝐢𝐨𝐧 𝐭𝐚𝐛𝐥𝐞 𝐭𝐡𝐚𝐭
𝐜𝐨𝐧𝐭𝐚𝐢𝐧𝐬 𝐢𝐧𝐟𝐨𝐫𝐦𝐚𝐭𝐢𝐨𝐧 𝐚𝐛𝐨𝐮𝐭 𝐭𝐡𝐞 𝐭𝐫𝐚𝐧𝐬𝐚𝐜𝐭𝐢𝐨𝐧𝐬 𝐜𝐚𝐫𝐫𝐢𝐞𝐝 𝐨𝐮𝐭 𝐛𝐲 𝐜𝐮𝐬𝐭𝐨𝐦𝐞𝐫𝐬.

𝐖𝐫𝐢𝐭𝐞 𝐚 𝐒𝐐𝐋 𝐪𝐮𝐞𝐫𝐲 𝐭𝐨 𝐣𝐨𝐢𝐧 𝐭𝐡𝐞𝐬𝐞 𝐭𝐰𝐨 𝐭𝐚𝐛𝐥𝐞𝐬 𝐚𝐧𝐝 𝐟𝐢𝐧𝐝 𝐭𝐡𝐞 𝐚𝐯𝐞𝐫𝐚𝐠𝐞 𝐭𝐫𝐚𝐧𝐬𝐚𝐜𝐭𝐢𝐨𝐧
𝐯𝐚𝐥𝐮𝐞 𝐟𝐨𝐫 𝐞𝐚𝐜𝐡 𝐜𝐮𝐬𝐭𝐨𝐦𝐞𝐫 𝐛𝐚𝐬𝐞𝐝 𝐨𝐧 𝐭𝐡𝐞 𝐩𝐚𝐲𝐦𝐞𝐧𝐭 𝐭𝐲𝐩𝐞.
'''



from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("CustomerSetup").getOrCreate()

customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True)
])

customer_data = [
    (1, "John", "Doe"),
    (2, "Jane", "Smith"),
    (3, "Bob", "Johnson")
]

transaction_details_schema = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("payment_type", StringType(), True),
    StructField("transaction_value", FloatType(), True)
])

transaction_details_data = [
    (101, 1, "Credit Card", 250.00),
    (102, 2, "Debit Card", 300.00),
    (103, 1, "Credit Card", 450.00),
    (104, 3, "Debit Card", 200.00),
    (105, 2, "Credit Card", 150.00),
    (106, 1, "Debit Card", 300.00)
]

df_transaction_details = spark.createDataFrame(transaction_details_data, transaction_details_schema)

df_customer = spark.createDataFrame(customer_data, customer_schema)

df_customer.join(df_transaction_details,on="customer_id",how="inner").\
    groupBy("customer_id","first_name","last_name","payment_type").agg(F.avg("transaction_value").alias("avg_transaction_value")).show()