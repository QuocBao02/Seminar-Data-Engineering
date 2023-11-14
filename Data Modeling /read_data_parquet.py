from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, DoubleType, ArrayType
import json

spark = SparkSession.builder \
    .appName("ETL to DataLake") \
    .config("hive.metastore.uris", "thrift://localhost:9084") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "3g") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()



#symbol_infor
hdfs_path = 'hdfs://localhost:9000/user/Binance_Data/lake/symbol_infor/year=2023/month=10/day=22/symbol_infor.parquet'
df = spark.read.parquet(hdfs_path, multiLine=True)
df.show()
row_count = df.count()
print(f"Số dòng trong DataFrame: {row_count}")

#ticker_24h
hdfs_path = 'hdfs://localhost:9000/user/Binance_Data/lake/ticker_24h/year=2023/month=10/day=22/ticker_24h.parquet'
df = spark.read.parquet(hdfs_path, multiLine=True)
df.show()
row_count = df.count()
print(f"Số dòng trong DataFrame: {row_count}")
# klines
hdfs_path = 'hdfs://localhost:9000/user/Binance_Data/lake/trades/year=2023/month=10/day=22/trades.parquet'
df = spark.read.parquet(hdfs_path, multiLine=True)
df.show()
row_count = df.count()
print(f"Số dòng trong DataFrame: {row_count}")
# data = json.loads(df.select("value").first()[0])
# # print(data)
# df = spark.createDataFrame(data)

hdfs_path = 'hdfs://localhost:9000/user/Binance_Data/lake/klines/year=2023/month=10/day=22/klines.parquet'
df = spark.read.parquet(hdfs_path, multiLine=True)
df.show()
row_count = df.count()
print(f"Số dòng trong DataFrame: {row_count}")  


# #symbol_infor
# hdfs_path = 'hdfs://localhost:9000/user/Binance_Data/lake/symbol_infor/year=2023/month=10/day=21/symbol_infor.parquet'
# df = spark.read.parquet(hdfs_path, multiLine=True)
# df.show()
# row_count = df.count()
# print(f"Số dòng trong DataFrame: {row_count}")

# #ticker_24h
# hdfs_path = 'hdfs://localhost:9000/user/Binance_Data/lake/ticker_24h/year=2023/month=10/day=21/ticker_24h.parquet'
# df = spark.read.parquet(hdfs_path, multiLine=True)
# df.show()
# row_count = df.count()
# print(f"Số dòng trong DataFrame: {row_count}")
# # klines
# hdfs_path = 'hdfs://localhost:9000/user/Binance_Data/lake/trades/year=2023/month=10/day=17/trades.parquet'
# df = spark.read.parquet(hdfs_path, multiLine=True)
# df.show()
# row_count = df.count()
# print(f"Số dòng trong DataFrame: {row_count}")
# data = json.loads(df.select("value").first()[0])
# # print(data)
# df = spark.createDataFrame(data)

# hdfs_path = 'hdfs://localhost:9000/user/Binance_Data/lake/klines/year=2023/month=10/day=21/klines.parquet'
# df = spark.read.parquet(hdfs_path, multiLine=True)
# df.show()
# row_count = df.count()
# print(f"Số dòng trong DataFrame: {row_count}")  
# spark.sql("CREATE DATABASE TESTV8;")
# x = spark.sql("SHOW DATABASES;")
# print("Địa chỉ lưu trữ của cơ sở dữ liệu mặc định:", spark.conf.get("spark.sql.warehouse.dir"))
# x.show()