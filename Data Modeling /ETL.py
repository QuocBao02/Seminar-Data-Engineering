from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, DoubleType, ArrayType
import json
# def _get_partition_time(self):
#     '''Create directory to save data into data lake in hadoop hdfs'''
#     # Get the current date and time
#     now = datetime.now()
#     partition=f'year={now.year}/month={now.month}/day={now.day}'
#     return partition
# create spark session 
# spark=SparkSession.builder.appName("Save to DataLake").getOrCreate()
spark = SparkSession.builder \
    .appName("ETL to DataLake") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "3g") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()

# Định nghĩa schema cho dữ liệu JSON
# custom_schema = StructType([
#     StructField("symbol", StringType(), True),
#     StructField("status", StringType(), True),
#     StructField("baseAsset", StringType(), True),
#     StructField("baseAssetPrecision", IntegerType(), True),
#     StructField("quoteAsset", StringType(), True),
#     StructField("quotePrecision", IntegerType(), True),
#     StructField("quoteAssetPrecision", IntegerType(), True),
#     StructField("baseCommissionPrecision", IntegerType(), True),
#     StructField("quoteCommissionPrecision", IntegerType(), True),
#     StructField("orderTypes", StringType(), True),
#     StructField("icebergAllowed", BooleanType(), True),
#     StructField("ocoAllowed", BooleanType(), True),
#     StructField("quoteOrderQtyMarketAllowed", BooleanType(), True),
#     StructField("allowTrailingStop", BooleanType(), True),
#     StructField("cancelReplaceAllowed", BooleanType(), True),
#     StructField("isSpotTradingAllowed", BooleanType(), True),
#     StructField("isMarginTradingAllowed", BooleanType(), True),
#     StructField("filters",StringType(), True),
#     StructField("permissions",StringType(), True),
#     StructField("defaultSelfTradePreventionMode", StringType(), True),
#     StructField("allowedSelfTradePreventionModes", StringType(), True),
# ])
    

# custom_schema =StructType([
#     StructField("value", ArrayType(StructType([
#         StructField("symbol", StringType()),
#         StructField("status", StringType()),
#         StructField("baseAsset", StringType()),
#         StructField("baseAssetPrecision", IntegerType()),
#         StructField("quoteAsset", StringType()),
#         StructField("quotePrecision", IntegerType()),
#         StructField("quoteAssetPrecision", IntegerType()),
#         StructField("baseCommissionPrecision", IntegerType()),
#         StructField("quoteCommissionPrecision", IntegerType()),
#         StructField("orderTypes", ArrayType(StringType())),
#         StructField("icebergAllowed", BooleanType()),
#         StructField("ocoAllowed", BooleanType()),
#         StructField("quoteOrderQtyMarketAllowed", BooleanType()),
#         StructField("allowTrailingStop", BooleanType()),
#         StructField("cancelReplaceAllowed", BooleanType()),
#         StructField("isSpotTradingAllowed", BooleanType()),
#         StructField("isMarginTradingAllowed", BooleanType()),
#         StructField("filters", ArrayType(StructType([
#             StructField("filterType", StringType()),
#             StructField("minPrice", StringType()),
#             StructField("maxPrice", StringType()),
#             StructField("tickSize", StringType()),
#             StructField("minQty", StringType()),
#             StructField("maxQty", StringType()),
#             StructField("stepSize", StringType()),
#             StructField("limit", IntegerType()),
#             StructField("minTrailingAboveDelta", IntegerType()),
#             StructField("maxTrailingAboveDelta", IntegerType()),
#             StructField("minTrailingBelowDelta", IntegerType()),
#             StructField("maxTrailingBelowDelta", IntegerType()),
#             StructField("bidMultiplierUp", StringType()),
#             StructField("bidMultiplierDown", StringType()),
#             StructField("askMultiplierUp", StringType()),
#             StructField("askMultiplierDown", StringType()),
#             StructField("avgPriceMins", IntegerType()),
#             StructField("minNotional", StringType()),
#             StructField("applyMinToMarket", BooleanType()),
#             StructField("maxNotional", StringType()),
#             StructField("applyMaxToMarket", BooleanType()),
#             StructField("maxNumOrders", IntegerType()),
#             StructField("maxNumAlgoOrders", IntegerType())
#         ]))),
#         StructField("permissions", ArrayType(StringType())),
#         StructField("defaultSelfTradePreventionMode", StringType()),
#         StructField("allowedSelfTradePreventionModes", (StringType()))
#     ])))
# ])


hdfs_path = 'hdfs://localhost:9000/user/Binance_Data/lake/klines/year=2023/month=10/day=13/klines.json'

df = spark.read.json(hdfs_path)
# from pyspark.sql.functions import from_json
# df = df.withColumn("value", from_json(df["value"],schema=custom_schema))
# Chọn các cột quan trọng
# df = df.select("value.*")

# Hiển thị kết quả
json.dumps(df.select("value").first()[0])
data = json.loads(df.select("value").first()[0])
# print(data)
df = spark.createDataFrame(data)
df.show(50)
# print(df.select("value").first()[0])
# df = spark.createDataFrame([df.select("value").first()[0]],  schema=custom_schema)
# # df = df.na.drop()
# # df.show()
# json_data = df.toJSON().collect()
# df.printSchema()
# In kết quả
# for row in json_data:
#     print(f"row\n")
# Thực hiện các biến đổi ETL (ví dụ: chọn một số cột)
# transformed_df = df.select("symbol", "status", "baseAsset")

# # # Hiển thị kết quả biến đổi
# transformed_df.show()


# spark = SparkSession.builder.appName("PySparkTest")\
#     .config("hive.metastore.uris", "thrift://localhost:8689")\
#     .config("spark.sql.warehouse.dir", "/home/hadoop/Binance_Market_Data")\
#     .enableHiveSupport()\
#     .getOrCreate()

# spark.sql("CREATE DATABASE test;")
# spark.sql("USE test;")
# spark.sql("CREATE TABLE table(id int, name string);")
# spark.sql("INSERT INTO TABLE test VALUES (1, 'Bao');")
# result=spark.sql("SELECT * FROM test")
# result.show()

# spark.stop()