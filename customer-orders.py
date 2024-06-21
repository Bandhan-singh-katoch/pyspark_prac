from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Customers Order")
sc = SparkContext(conf=conf)

def parseCsv(line):
    fields = line.split(',')
    return (int(fields[0]),float(fields[2]))

input = sc.textFile("customer-orders.csv")
parsedInput = input.map(parseCsv)
result = parsedInput.reduceByKey(lambda x,y: x+y)
sortedResult = result.map(lambda x: (x[1],x[0])).sortByKey()

for price,id in sortedResult.collect():
    print(f"{id}  {price}")

# print(result.collect())