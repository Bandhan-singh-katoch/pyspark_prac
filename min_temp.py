from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("1800.csv")
parsedLine = lines.map(parseLine)
minTemps = parsedLine.filter(lambda x: 'TMIN' in x[1])
stationTemp = minTemps.map(lambda x: (x[0],x[2]))
minTemps = stationTemp.reduceByKey(lambda x,y: min(x,y))
for result in minTemps.collect():
    print(result[0]+"\t{:.2f}F".format(result[1]))



# funtion to parseLine stationID, entryType, temperature
# read csv and parse 
# filter minTemps
# get stationID and temp only
# find min for each station 
# loop