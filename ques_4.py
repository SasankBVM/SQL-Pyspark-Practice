'''
An e-commerce platform stores information about its customers and their purchase history.

Some customers may have registered on the platform but have not placed any orders yet.



You are given two tables:



Customers

╔═════════════╦══════════╗
║ Column Name ║   Type   ║
╠═════════════╬══════════╣
║ customer_id ║   int    ║
║─────────────┼──────────║
║    name     ║ varchar  ║
╚═════════════╩══════════╝
customer_id is the primary key.
Each row represents a registered customer.


Orders

╔═════════════╦══════════╗
║ Column Name ║   Type   ║
╠═════════════╬══════════╣
║  order_id   ║   int    ║
║─────────────┼──────────║
║ customer_id ║   int    ║
║─────────────┼──────────║
║   amount    ║   int    ║
╚═════════════╩══════════╝
order_id is the primary key.
customer_id references Customers.customer_id.


Write an SQL query to return all customers, along with their order details. If a customer has not placed any orders, display NULL for order-related columns.

The result can be returned in any order.
'''
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

spark = SparkSession.builder.appName("ques_4").master("local[*]").getOrCreate()
schema1 = StructType([
    StructField("customer_id",IntegerType()),
    StructField("name",StringType())
])
customers = spark.createDataFrame(
    [(1,"Alice"),(2,"Bob"),(3,"Charlie")], schema1
)

schema2 = StructType([
    StructField("order_id",IntegerType()),
    StructField("customer_id",IntegerType()),
    StructField("amount",IntegerType())
])
orders = spark.createDataFrame(
    [
        (101,1,500),
        (102,1,300),
        (103,2,200)
    ], schema2
)
orders.show()

customers.join(orders,on="customer_id",how="leftouter").select(["customer_id","name","order_id","amount"]).show()

