from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, size, split, sum,col,min 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("Popular Hero").getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

names = spark.read.schema(schema).option("sep", " ").csv("Marvel+Names")
lines = spark.read.text("Marvel+Graph")

connections = lines.withColumn("id", split(trim(col("value")), " ")[0])\
              .withColumn("connections", size(split(trim(col("value"))," "))-1)\
              .groupBy("id").agg(sum("connections").alias("connections"))

# mostPopular = connections.sort(col("connections").desc()).first()
# mostPopularName = names.filter(names.id == mostPopular.id).select("name").first()

lessPopularCount = connections.agg(min("connections")).first()[0]
lessPopular = connections.filter(connections.connections == lessPopularCount)

lessPopularNames = lessPopular.join(names, "id")
lessPopularNames.select("name").show()

spark.stop()