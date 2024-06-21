from pyspark import SparkConf, SparkContext
import re 
conf = SparkConf().setMaster("local").setAppName("Word count")
sc = SparkContext(conf=conf)

def normalizeWord(text):
    return re.compile(r'\W+',re.UNICODE).split(text.lower())

input = sc.textFile("book.txt")
# words = input.flatMap(lambda x: x.split())
words = input.flatMap(normalizeWord)
# wordCounts = words.countByValue()
wordCounts = words.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
print(wordCountsSorted.collect())
# results

for word,count in wordCountsSorted.collect():
    cleanWord = word.encode("ascii","ignore")
    if (cleanWord):
        print (cleanWord.decode(),count)