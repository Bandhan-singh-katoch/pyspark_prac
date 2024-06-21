from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create a SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), \
               age=int(fields[2]), numFriends=int(fields[3]))

lines = spark.sparkContext.textFile("fakefriends.csv")
people = lines.map(mapper)
print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
# print(people.collect())
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("peoples")

teenagers = spark.sql("select * from peoples where age>=13 and age <=19")

for teenager in teenagers.collect():
    print(teenager)

schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()