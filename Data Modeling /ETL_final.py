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

hdfs_path = 'hdfs://localhost:9000/user/Binance_Data/lake/symbol_infor/year=2023/month=10/day=13/symbol_infor.json'
df = spark.read.json(hdfs_path)
json.dumps(df.select("value").first()[0])
data = json.loads(df.select("value").first()[0])
df = spark.createDataFrame(data)
df.select("symbol", "status", "baseAsset", "baseAssetPrecision", "quoteAsset","quotePrecision", "icebergAllowed", "orderTypes" ).show(50)

#ticker_24h
hdfs_path = 'hdfs://localhost:9000/user/Binance_Data/lake/ticker_24h/year=2023/month=10/day=13/ticker_24h.json'

df = spark.read.json(hdfs_path)
json.dumps(df.select("value").first()[0])
data = json.loads(df.select("value").first()[0])
# print(data)
df = spark.createDataFrame(data)
df.show(50)

#trades
hdfs_path = 'hdfs://localhost:9000/user/Binance_Data/lake/trades/year=2023/month=10/day=13/trades.json'

df = spark.read.json(hdfs_path)
json.dumps(df.select("value").first()[0])
data = json.loads(df.select("value").first()[0])
# print(data)
df = spark.createDataFrame(data)
df.show(50)

#klines
hdfs_path = 'hdfs://localhost:9000/user/Binance_Data/lake/klines/year=2023/month=10/day=13/klines.json'

df = spark.read.json(hdfs_path)
json.dumps(df.select("value").first()[0])
data = json.loads(df.select("value").first()[0])
# print(data)
df = spark.createDataFrame(data)
df.show(50)