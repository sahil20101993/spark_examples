import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
list1 = [(1, 'abc'),(2,'def')]
olddf = spark.createDataFrame(list1, ['id', 'value'])
olddf.show();
list2 = [(2, 'cde'),(3,'xyz')]
newdf = spark.createDataFrame(list2, ['id', 'value'])
newdf.show();

df = olddf.join(newdf, olddf.id == newdf.id, 'full_outer').select(coalesce(olddf.id, newdf.id).alias("id"),\
                                                                  coalesce(newdf.value, olddf.value).alias("value"))

df.show();
