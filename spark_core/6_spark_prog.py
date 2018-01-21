import os
import numpy as np
import sys
import logging

LOG_PATH = os.environ['log']

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test") \
   .getOrCreate()

logger = logging.getLogger(__name__)
logger.addHandler(logging.FileHandler(LOG_PATH))

def main(*args):
    top = int(args[0][0])
    data = spark.read.csv("hdfs:///tmp/ratings.csv", sep = ',', header= True)
    result = (data
               .groupBy("movieid")
               .agg({'rating': 'mean'})
               .withColumnRenamed("avg(rating)", "avg_ratings")
               .dropna()
               .orderBy(['avg_ratings'], ascending=[0])
	       .limit(top))
    logger.info("result: {}".format(result.toPandas()))
    spark.stop()

if __name__ == '__main__':
    logging.basicConfig(format='[%(levelname)s] %(asctime)s %(message)s',
    	datefmt='%Y-%m-%d %H:%M:%S',
    	level=logging.INFO)
    main(sys.argv[1:])
