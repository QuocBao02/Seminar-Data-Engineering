from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Read_data").getOrCreate()
data=spark.read.parquet("/home/quocbao/Downloads/data.parquet")

data.show()

spark.stop()