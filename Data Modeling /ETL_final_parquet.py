from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, DoubleType, ArrayType
import json

spark = SparkSession.builder \
    .appName("ETL to DataLake") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "3g") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()

#symbol_infor
hdfs_path = 'hdfs://localhost:9000/user/Binance_Data/lake/symbol_infor/year=2023/month=10/day=13/symbol_infor.parquet'
df = spark.read.parquet(hdfs_path, multiLine=True)
df.show()
row_count = df.count()
print(f"Số dòng trong DataFrame: {row_count}")

#ticker_24h
hdfs_path = 'hdfs://localhost:9000/user/Binance_Data/lake/ticker_24h/year=2023/month=10/day=13/ticker_24h.parquet'
df = spark.read.parquet(hdfs_path, multiLine=True)
df.show()
row_count = df.count()
print(f"Số dòng trong DataFrame: {row_count}")
# klines
hdfs_path = 'hdfs://localhost:9000/user/Binance_Data/lake/trades/year=2023/month=10/day=13/trades.parquet'
df = spark.read.parquet(hdfs_path, multiLine=True)
df.show()
row_count = df.count()
print(f"Số dòng trong DataFrame: {row_count}")
# data = json.loads(df.select("value").first()[0])
# # print(data)
# df = spark.createDataFrame(data)

hdfs_path = 'hdfs://localhost:9000/user/Binance_Data/lake/klines/year=2023/month=10/day=13/klines.parquet'
df = spark.read.parquet(hdfs_path, multiLine=True)
df.show()
row_count = df.count()
print(f"Số dòng trong DataFrame: {row_count}")