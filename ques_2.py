'''
A platform stores user information in two separate tables:

One for currently active users
One for archived users
To generate a consolidated report, the company wants a unique list of all user IDs that have ever existed on the platform.



ActiveUsers

╔═════════════╦══════════╗
║ Column Name ║   Type   ║
╠═════════════╬══════════╣
║   user_id   ║   int    ║
╚═════════════╩══════════╝
user_id is the primary key.


ArchivedUsers

╔═════════════╦══════════╗
║ Column Name ║   Type   ║
╠═════════════╬══════════╣
║   user_id   ║   int    ║
╚═════════════╩══════════╝
user_id is the primary key.
Some users may appear in both tables.


Write an SQL query to return all unique user_ids that appear in either ActiveUsers or ArchivedUsers. Return the result in any order.




Example 1

Input:



ActiveUsers:

╔══════════╗
║ user_id  ║
╠══════════╣
║    1     ║
║──────────║
║    2     ║
║──────────║
║    3     ║
╚══════════╝


ArchivedUsers:

╔══════════╗
║ user_id  ║
╠══════════╣
║    3     ║
║──────────║
║    4     ║
║──────────║
║    5     ║
╚══════════╝


Output:

╔══════════╗
║ user_id  ║
╠══════════╣
║    1     ║
║──────────║
║    2     ║
║──────────║
║    3     ║
║──────────║
║    4     ║
║──────────║
║    5     ║
╚══════════╝


Explanation

User 3 appears in both tables but is shown only once in the result.
'''

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

# Start Spark Session if you haven't already
spark = SparkSession.builder.appName("UserDataFrames").getOrCreate()

# Define a shared schema since both tables only contain integer IDs
schema = StructType([
    StructField("user_id", IntegerType(), True)
])

# 1. Create ActiveUsers DataFrame (Contains 1, 2, 3)
active_data = [(1,), (2,), (3,)]
df_active = spark.createDataFrame(active_data, schema=schema)

# 2. Create ArchivedUsers DataFrame (Contains 3, 4, 5)
archived_data = [(3,), (4,), (5,)]
df_archived = spark.createDataFrame(archived_data, schema=schema)

df_active.union(df_archived).show()