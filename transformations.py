
import pyspark
from pyspark.sql import SparkSession
import psutil
from pyspark.sql.types import StructType, StructField, StringType, Row

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
rdd = spark.sparkContext.textFile("D:\\Spark\\data\\sahil_data\\test.txt")
print("Hi" , rdd.collect())

rdd2 = rdd.flatMap(lambda x: x.split(" "))
print("1", rdd2.collect())

rdd3 = rdd2.map(lambda x: (x,1))
print("2",rdd3.collect())

rdd5 = rdd3.reduceByKey(lambda a,b: a+b)
print("3",rdd5.collect())

rdd6 = rdd5.map(lambda x: (x[1],x[0])).sortByKey()
#Print rdd6 result to console
print("4",rdd6.collect())

rdd4 = rdd.filter(lambda x : 'an' in x[1])
print("5",rdd4.collect())