import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("parquetFile").getOrCreate()
data =[("James ","","Smith","36636","M",3000),
              ("Michael ","Rose","","40288","M",4000),
              ("Robert ","","Williams","42114","M",4000),
              ("Maria ","Anne","Jones","39192","F",4000),
              ("Jen","Mary","Brown","","F",-1)]

columns=["firstname","middlename","lastname","dob","gender","salary"]
df=spark.createDataFrame(data,columns)

df.write.format("avro").mode("append").save("C:\\tmp\\spark_out\\avro\\person.avro")

spark.read.format("avro").load("/tmp/spark_out/avro/person.avro").show()
df.write.partitionBy("dob_year", "dob_month").format("avro").mode("append").save("/tmp/spark_out/avro/person_partition.avro")

spark.read.format("avro").load("/tmp/spark_out/avro/person_partition.avro").where(col("dob_year") == 2010).show()

#schemaAvro = new Schema.Parser().parse(new File("src/main/resources/person.avsc"))


#spark.read.format("avro").option("avroSchema", schemaAvro.toString).load("/tmp/spark_out/avro/person.avro").show()

spark.sqlContext.sql("CREATE TEMPORARY VIEW PERSON USING avro OPTIONS (path \"/tmp/spark_out/avro/person.avro\")")

spark.sqlContext.sql("SELECT * FROM PERSON").show()

