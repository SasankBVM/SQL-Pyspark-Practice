'''
We have a table named Employees that contains information about employees and their managers.



Table: Employees



+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| employee_id   | int     |
| employee_name | varchar |
| manager_id    | int     |
+---------------+---------+


employee_id is the primary key for this table and contains unique values for each employee.
Each row in this table represents an employee who reports to a direct manager identified by manager_id.
The head of the company is the employee with employee_id = 1.


Task:

Write a query to find the employee_id of all employees that directly or indirectly report their work to the head of the company. Order the result by employee_id in ascending order.


Example 1

Example:



Input:

Employees table:



+-------------+---------------+------------+
| employee_id | employee_name | manager_id |
+-------------+---------------+------------+
| 1           | Boss          | 1          |
| 3           | Alice         | 3          |
| 2           | Bob           | 1          |
| 4           | Daniel        | 2          |
| 7           | Luis          | 4          |
| 8           | Jhon          | 3          |
| 9           | Angela        | 8          |
| 77          | Robert        | 1          |
+-------------+---------------+------------+


Output:



+-------------+
| employee_id |
+-------------+
| 2           |
| 4           |
| 7           |
| 77          |
+-------------+


Explanation:

The head of the company is the employee with employee_id = 1.
Employees with employee_id 2 and 77 report directly to the head of the company.
Employee 4 reports indirectly to the head of the company through 4 → 2 → 1.
Employee 7 reports indirectly to the head of the company through 7 → 4 → 2 → 1.
Employees with employee_id 3, 8, and 9 do not report directly or indirectly to the head of the company.
'''

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Start the Spark session
spark = SparkSession.builder.appName("EmployeeHierarchy").getOrCreate()

# ==========================================
# EMPLOYEES SCHEMA
# ==========================================
employees_schema = StructType([
    StructField("employee_id", IntegerType(), False),
    StructField("employee_name", StringType(), False),
    StructField("manager_id", IntegerType(), True)
])

# ==========================================
# EMPLOYEES DATA (from the image)
# ==========================================
employees_data = [
    (1, "Boss", 1),
    (3, "Alice", 3),
    (2, "Bob", 1),
    (4, "Daniel", 2),
    (7, "Luis", 4),
    (8, "Jhon", 3),
    (9, "Angela", 8),
    (77, "Robert", 1)
]

df_employees = spark.createDataFrame(employees_data, employees_schema)

# ==========================================
# Show the results
# ==========================================
print("Employees DataFrame:")
df_employees.createOrReplaceTempView("employees")
spark.sql("""
          with recursive cte_data as(
              select employee_id from
              employees where manager_id=1 and employee_id <> 1
              union all
              select e.employee_id as employee_id
              from
              cte_data c
              inner join
              employees e
              on c.employee_id = e.manager_id
          )
          
          select employee_id
          from
          cte_data
          where employee_id <> 1
          order by employee_id
          """).show()