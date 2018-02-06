from __future__ import print_function, division
import os
import sys 

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession.builder.getOrCreate()

lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", '0.0.0.0:9092')\
        .option("subscribe", "t1")\
        .load()\
        .selectExpr("CAST(value AS STRING)")

words = lines.select(
        explode(
            split(lines.value, ' ')
        ).alias('word')
    )

wordCounts = words.groupBy('word').count()

query = wordCounts\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .start()

query.awaitTermination()
