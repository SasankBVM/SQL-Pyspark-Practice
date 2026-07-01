'''
Hierarchical structures like file systems, organizational charts, or comment threads in a forum often need to differentiate between root, inner, and leaf nodes. Proper classification helps in rendering or processing the hierarchy efficiently.



You are given a table Tree:



╔═════════════╦══════════╗
║ Column Name ║   Type   ║
╠═════════════╬══════════╣
║     id      ║   int    ║
║─────────────┼──────────║
║    p_id     ║   int    ║
╚═════════════╩══════════╝


id: The unique ID of a node (Primary Key).
p_id: The ID of the parent node for this node.
A node where p_id IS NULL represents the root of the tree (since it has no parent).
The table forms a valid tree (i.e., it is connected and acyclic with one root).


Each node can be classified as:



╔═══════╦═════════════════════════════════════════════════════════════════╗
║ Type  ║                           Description                           ║
╠═══════╬═════════════════════════════════════════════════════════════════╣
║ Root  ║ If p_id IS NULL (i.e., no parent)                               ║
║───────┼─────────────────────────────────────────────────────────────────║
║ Leaf  ║ If the node does not appear as a parent of any other node       ║
║───────┼─────────────────────────────────────────────────────────────────║
║ Inner ║ If the node has a parent (p_id is not NULL) and is also a       ║
║       ║ parent to at least one other node                               ║
╚═══════╩═════════════════════════════════════════════════════════════════╝


Write a query to determine the type of each node in a tree structure. Each node should be labeled as one of the following:

'Root': if it has no parent
'Leaf': if it has no children
'Inner': if it has both a parent and children
The output can be in any order.
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when,lit
from pyspark.sql.types import StructType, StructField, IntegerType

# 1. Initialize the Spark Session
spark = SparkSession.builder \
    .appName("CreateTreeTable") \
    .getOrCreate()

# 2. Define the schema (IntegerType allows for None/null values)
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("p_id", IntegerType(), True)
])

# 3. Create the data rows (Use Python's 'None' to represent 'null')
data = [
    (1, None),
    (2, 1),
    (3, 1),
    (4, 2),
    (5, 2)
]

# 4. Generate the DataFrame
df_tree = spark.createDataFrame(data, schema=schema)

# 5. Display the result
parent_ids = [row[0] for row in df_tree.select("p_id").dropna().collect()]


df_tree.select([col("id"),when(col("p_id").isNull(),lit("Root")).
               when((col("p_id").isNotNull()) & (col("id").isin(parent_ids)), lit("Inner"))
               .otherwise(lit("Leaf")).alias("Type_of_Node")]).show()