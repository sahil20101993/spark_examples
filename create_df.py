import pyspark
from pyspark import Row
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("create dataframe").getOrCreate()

columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
rdd = spark.sparkContext.parallelize(data)

dfFromRDD1 = rdd.toDF()
dfFromRDD1.printSchema()

dfFromRDD1 = rdd.toDF(columns)
dfFromRDD1.printSchema()

dfFromRDD2 = spark.createDataFrame(rdd).toDF(*columns)
dfFromRDD1.show()
print(dfFromRDD1)

dfFromData2 = spark.createDataFrame(data).toDF(*columns)
rowData = map(lambda x: Row(*x), data)
print(rowData)
dfFromData3 = spark.createDataFrame(rowData,columns)
dfFromData3.show()
