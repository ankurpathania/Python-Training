from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Python Spark SQL basic example").getOrCreate()

df_pyspark = spark.read.option('header','true').csv('test.csv')
df_pyspark.show()

