import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
from pyspark.sql.functions import when,expr

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [('James', '', 'Smith', '1991-04-01', 'M', 3000),
        ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
        ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
        ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
        ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)
        ]

columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
df = spark.createDataFrame(data=data, schema=columns)
df.printSchema()
df.show(truncate=False)

df2 = df.withColumn("new_gender", when(col("gender") == "M","Male").when(col("gender") == "F","Female").otherwise("Unknown"))
df2.show(truncate=False)

df4 = df.select(col("*"), when(col("gender") == "M","Male").when(col("gender") == "F","Female").otherwise("Unknown").alias("new_gender"))
df4.show(truncate=False)

df5 = df.withColumn("new_gender",expr("case when gender = 'M' then 'Male' " +"when gender = 'F' then 'Female' " + "else 'Unknown' end"))
df5.show(truncate=False)

df6 = df.select(col("*"),expr("case when gender = 'M' then 'Male' " +"when gender = 'F' then 'Female' " +"else 'Unknown' end").alias("new_gender"))
df6.show(truncate=False)


