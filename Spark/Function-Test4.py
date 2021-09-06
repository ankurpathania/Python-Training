from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Python Spark SQL basic example").getOrCreate()

df_pyspark = spark.read.option('header','true').csv('test.csv')
#df_pyspark.show()
## For Mean
from pyspark.ml.feature import Imputer
imputer = Imputer(
     inputCols=['Age','Experience','Salary'],
     outputCols=["{}_imputed".format(c) for c in ['Age','Experience','Salary']]
).setStrategy("median")

imputer.fit(df_pyspark).transform(df_pyspark).show()

