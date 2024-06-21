from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Sql dataframe").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
people = spark.read.option("header", "true").option("inferSchema", "true").csv("fakefriends-header.csv")

# print("Infered schema")
# people.printSchema()

# print("select particular schema")
# people.select(people.name).show()

# print("with filter")
# people.filter((people.age>30) & (people.age<40)).show()

# print("group by    ")
# people.groupBy("age").count().filter("count >10").show()

print("add 10 in age")
people.select(people.age, people.age+10).show()
spark.stop()