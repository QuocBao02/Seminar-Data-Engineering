from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DecimalType
import json 
# Khởi tạo phiên làm việc Spark
# spark = SparkSession.builder.appName("Read from HDFS").getOrCreate()
spark = SparkSession.builder \
            .appName("Save to DataLake") \
            .config("spark.driver.memory", "6g") \
            .config("spark.executor.memory", "3g") \
            .config("spark.executor.cores", "4") \
            .getOrCreate()
# Đường dẫn trên HDFS đến tệp bạn muốn đọchdfs_path = 'hdfs://localhost:9000/user/Binance_Data/lake/klines/year=2023/month=10/day=23/klines.parquet'
hdfs_path = 'hdfs://localhost:9000/user/Binance_Data/lake/symbol_infor/year=2023/month=11/day=20/symbol_infor.parquet'
df = spark.read.parquet(hdfs_path, multiLine=True)
df.show(10)
row_count = df.count()
print(f"Số dòng trong DataFrame: {row_count}")

hdfs_path = 'hdfs://localhost:9000/user/Binance_Data/lake/ticker_24h/year=2023/month=11/day=20/ticker_24h.parquet'
df = spark.read.parquet(hdfs_path, multiLine=True)
df.show(10)
row_count = df.count()
print(f"Số dòng trong DataFrame: {row_count}")

hdfs_path = 'hdfs://localhost:9000/user/Binance_Data/lake/trades/year=2023/month=11/day=20/trades.parquet'
df = spark.read.parquet(hdfs_path, multiLine=True)
df.show(10)
row_count = df.count()
print(f"Số dòng trong DataFrame: {row_count}")

hdfs_path = 'hdfs://localhost:9000/user/Binance_Data/lake/klines/year=2023/month=11/day=20/klines.parquet'
df = spark.read.parquet(hdfs_path, multiLine=True)
df.show(10)
row_count = df.count()
print(f"Số dòng trong DataFrame: {row_count}")
spark.stop()

