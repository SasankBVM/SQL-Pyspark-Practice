'''

𝐀𝐬 𝐚𝐧 𝐚𝐧𝐚𝐥𝐲𝐬𝐭 𝐚𝐭 𝐌𝐚𝐬𝐭𝐞𝐫𝐜𝐚𝐫𝐝, 𝐲𝐨𝐮 𝐡𝐚𝐯𝐞 𝐚𝐜𝐜𝐞𝐬𝐬 𝐭𝐨 𝐚 𝐝𝐚𝐭𝐚𝐛𝐚𝐬𝐞 𝐭𝐡𝐚𝐭 𝐜𝐨𝐧𝐭𝐚𝐢𝐧𝐬 
𝐜𝐮𝐬𝐭𝐨𝐦𝐞𝐫 𝐢𝐧𝐟𝐨𝐫𝐦𝐚𝐭𝐢𝐨𝐧. 𝐘𝐨𝐮 𝐡𝐚𝐯𝐞 𝐛𝐞𝐞𝐧 𝐚𝐬𝐤𝐞𝐝 𝐭𝐨 𝐫𝐞𝐭𝐫𝐢𝐞𝐯𝐞 𝐫𝐞𝐜𝐨𝐫𝐝𝐬 𝐨𝐟
𝐜𝐮𝐬𝐭𝐨𝐦𝐞𝐫𝐬 𝐰𝐡𝐨 𝐚𝐫𝐞 𝐟𝐫𝐨𝐦 𝐔𝐒𝐀, 𝐡𝐚𝐯𝐞 𝐚𝐭 𝐥𝐞𝐚𝐬𝐭 𝐭𝐰𝐨 𝐝𝐢𝐟𝐟𝐞𝐫𝐞𝐧𝐭 𝐜𝐚𝐫𝐝𝐬 𝐰𝐢𝐭𝐡 𝐮𝐬


'''



from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("CustomerCardTransactionSetup").getOrCreate()

customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_name", StringType(), True),
    StructField("country", StringType(), True)
])

customer_data = [
    (100, "John Doe", "USA"),
    (101, "Jane Smith", "USA"),
    (102, "Roberto Martinez", "Mexico"),
    (103, "Francois Dupont", "France")
]

df_customer = spark.createDataFrame(customer_data, customer_schema)

card_schema = StructType([
    StructField("card_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("card_type", StringType(), True)
])

card_data = [
    (5001, 100, "Mastercard Credit"),
    (5002, 100, "Mastercard Debit"),
    (5003, 101, "Mastercard Credit"),
    (5004, 102, "Mastercard Credit"),
    (5005, 102, "Mastercard Debit")
]

df_card = spark.createDataFrame(card_data, card_schema)

df_customer.show()
df_card.show()

df_customer.join(df_card,on="customer_id",how="inner").\
    groupBy("customer_id","customer_name","country").agg(F.count_distinct(F.col("card_type")).alias("count_of_card_types")).\
    where((F.col("count_of_card_types") >= 2) | (F.col("country") == "USA")).show()