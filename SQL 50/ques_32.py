from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

student_schema = StructType([
    StructField("student_id", IntegerType(), True),
    StructField("student_name", StringType(), True)
])

student_data = [
    (1, "Daniel"),
    (2, "Jade"),
    (3, "Stella"),
    (4, "Jonathan"),
    (5, "Will")
]

student_df = spark.createDataFrame(student_data, schema=student_schema)

exam_schema = StructType([
    StructField("exam_id", IntegerType(), True),
    StructField("student_id", IntegerType(), True),
    StructField("score", IntegerType(), True)
])

exam_data = [
    (10, 1, 70),
    (10, 2, 80),
    (10, 3, 90),
    (20, 1, 80),
    (30, 1, 70),
    (30, 3, 80),
    (30, 4, 90),
    (40, 1, 60),
    (40, 2, 70),
    (40, 4, 80)
]

exam_df = spark.createDataFrame(exam_data, schema=exam_schema)

stu_took_exam = student_df.join(exam_df,on="student_id",how = "inner")

window_spec = Window.partitionBy("exam_id")

top_candidates = exam_df.withColumn("max_score", F.max("score").over(window_spec)).withColumn("min_score",F.min("score").over(window_spec))\
    .withColumn("first_last_candidate", F.when(F.col("score")==F.col("max_score"), F.lit(F.col("student_id"))).when(F.col("score")==F.col("min_score"),F.lit(F.col("student_id")))
    ).filter(F.col("first_last_candidate").isNotNull()).select("first_last_candidate").distinct()

stu_took_exam.join(top_candidates, on = stu_took_exam["student_id"] == top_candidates["first_last_candidate"],how="anti").select("student_id","student_name").distinct().show()

exam_df.createTempView("e_df")
student_df.createTempView("s_df")

spark.sql(""" 
          with exam_givers
          (
            select a.student_id as student_id, student_name, score,exam_id
            from
            s_df a 
            inner join
            e_df b
            on a.student_id = b.student_id
          ),
          max_min_scores as
          (
              select student_id,score,student_name, min(score) over(partition by exam_id) as min_score, max(score) over(partition by exam_id) as max_score
              from
              exam_givers
          ),
          toppers_least_scorers as 
          (
              select distinct student_id, student_name
              from
              max_min_scores
              where score = min_score or score = max_score
          )
          
          select distinct a.student_id as student_id, a.student_name as student_name
          from
          exam_givers a
          left join 
          toppers_least_scorers b
          on a.student_id = b.student_id
          where b.student_id is null
          
          """).show()