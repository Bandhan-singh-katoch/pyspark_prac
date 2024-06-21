from pyspark.sql import SparkSession
from pyspark.sql.functions import asc,round,avg

spark = SparkSession.builder.appName("Friends by age using dataframe").getOrCreate()

people = spark.read.option("header","true").option("inferSchema","true").csv("fakefriends-header.csv")
people.show()

# people.groupBy("age").avg("friends").alias("average age").orderBy(asc("age")).show()
people.groupBy("age").agg(round(avg("friends"),2)).alias("average_age").sort("age").show()

spark.stop()