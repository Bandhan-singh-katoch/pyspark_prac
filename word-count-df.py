from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower,asc

spark = SparkSession.builder.appName("Word count df").getOrCreate()

inputDf = spark.read.text("book.txt")

words = inputDf.select(explode(split(inputDf.value, "\\W+")).alias("word"))
wordWithoutEmptyString = words.filter(words.word != '')
lowerCaseWord = wordWithoutEmptyString.select(lower(wordWithoutEmptyString.word).alias("word"))

wordCount = lowerCaseWord.groupBy("word").count().alias("count")
orderdCount = wordCount.orderBy(asc("count"))
orderdCount.show()
spark.stop()
