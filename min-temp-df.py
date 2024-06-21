from pyspark.sql import SparkSession
from pyspark.sql.functions import round, col, min
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("Min temp df").getOrCreate()

schema = StructType([
    StructField("stationId",StringType(), True),
    StructField("date", IntegerType(), True),
    StructField("measureType", StringType(), True),
    StructField("temperature", FloatType(), True)
])
df = spark.read.schema(schema).csv("1800.csv")

minTemp = df.filter(df.measureType == "TMIN")
requiredData = minTemp.select(minTemp.stationId, minTemp.temperature)

minTempByStation = requiredData.groupBy("stationId").agg(min("temperature").alias("minTemp"))

tempInFah = minTempByStation.withColumn("temperature", round(col("minTemp")*0.1*(9.0/5.0)+32,2))
tempInFah.show()
spark.stop()