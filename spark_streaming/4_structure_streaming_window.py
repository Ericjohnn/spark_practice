from __future__ import print_function, division
import os
import sys 

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

windowDuration, slideDuration = '3 seconds', '1 seconds'


lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .option('includeTimestamp', 'true')\
    .load()

# Split the lines into words
words = lines.select(
   explode(
       split(lines.value, " ")).alias("word"), 
   lines.timestamp
)

words = words.withWatermark("timestamp", "5 seconds")\

windowedCounts = words\
        .groupBy(
           window(words.timestamp, windowDuration, slideDuration),
           words.word)\
        .count()

query = windowedCounts \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()\
    .awaitTermination()
