import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MovieReviewsSetup").getOrCreate()

movie_reviews_schema = StructType([
    StructField("REVIEW_ID", IntegerType(), False),
    StructField("MOVIE_NAME", StringType(), True),
    StructField("USER_ID", IntegerType(), True),
    StructField("RATING", IntegerType(), True),
    StructField("RATING_DATE", StringType(), True)
])

movie_reviews_data = [
    (1, "Spider-Man: Brand New Day", 201, 5, "03-JUL-26"),
    (2, "Spider-Man: Brand New Day", 202, 4, "08-JUL-26"),
    (3, "Spider-Man: Brand New Day", 203, 5, "18-JUL-26"),
    (4, "Superman", 204, 4, "05-JUL-26"),
    (5, "Superman", 205, 3, "14-JUL-26"),
    (6, "Fantastic Four", 206, 5, "09-JUL-26"),
    (7, "Fantastic Four", 207, 4, "21-JUL-26"),
    (8, "Spider-Man: Brand New Day", 208, 4, "02-AUG-26"),
    (9, "Spider-Man: Brand New Day", 209, 5, "11-AUG-26"),
    (10, "Spider-Man: Brand New Day", 214, 3, "28-AUG-26"),
    (11, "Superman", 210, 5, "06-AUG-26"),
    (12, "Superman", 211, 4, "19-AUG-26"),
    (13, "Superman", 215, 5, "30-AUG-26"),
    (14, "Fantastic Four", 212, 3, "04-AUG-26"),
    (15, "Fantastic Four", 213, 4, "23-AUG-26")
]

df_movie_reviews = spark.createDataFrame(movie_reviews_data, movie_reviews_schema)

# Format 1

df_movie_reviews.withColumn("Month",F.split("RATING_DATE","-").getItem(1)).\
    withColumn("Year",F.split("RATING_DATE","-").getItem(2)).\
        groupBy("MOVIE_NAME","Year","Month").agg(F.round(F.avg("RATING"),2).alias("Avg_Rating")).show()
        

# Format 2

df_movie_reviews.withColumn("Year",F.year(F.to_date("RATING_DATE","dd-MMM-yy"))).\
    withColumn("Month",F.month(F.to_date("RATING_DATE","dd-MMM-yy"))).\
        groupBy("MOVIE_NAME","Year","Month").agg(F.round(F.avg("RATING"),2).alias("Avg_Rating")).show()