from pyspark.sql import SparkSession
import pyspark
spark: SparkSession = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
sparkContext=spark.sparkContext
# Create RDD from parallelize
data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
rdd = spark.sparkContext.parallelize(data)
rddCollect = rdd.collect()
print("Number of Partitions: " + str(rdd.getNumPartitions()))
print("Action: First element: " + str(rdd.first()))
print(rddCollect)

emptyRDD = sparkContext.emptyRDD()
emptyRDD2 =sparkContext.parallelize([])

print("is Empty RDD : "+str(emptyRDD2.isEmpty()))


rdd4 = spark.sparkContext.parallelize((0,20))
print("From local[5]"+str(rdd4.getNumPartitions()))

rdd1 = spark.sparkContext.parallelize((0,25), 6)
print("parallelize : "+str(rdd1.getNumPartitions()))

rdd2 = rdd1.repartition(4)
print("Repartition size : " + str(rdd2.getNumPartitions()))