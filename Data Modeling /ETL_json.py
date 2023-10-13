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


spark = SparkSession.builder \
    .appName("ETL to DataLake") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("spark.driver.memory", "10g") \
    .config("spark.executor.memory", "10g") \
    .config("spark.executor.cores", "8") \
    .getOrCreate()

hdfs_path = 'hdfs://localhost:9000/user/Binance_Data/lake/symbol_infor/year=2023/month=10/day=13/symbol_infor.json'
df = spark.read.json(hdfs_path, multiLine=True)
#json.dumps(df.select("value").first()[0])
data = json.loads(df.select("value").first()[0])
df = spark.createDataFrame(data)
# df.select("symbol", "status", "baseAsset", "baseAssetPrecision", "quoteAsset","quotePrecision", "icebergAllowed", "orderTypes" ).show(50)
df.show()
#ticker_24h

hdfs_path = 'hdfs://localhost:9000/user/Binance_Data/lake/ticker_24h/year=2023/month=10/day=13/ticker_24h.json'

df = spark.read.json(hdfs_path, multiLine=True)
# json.dumps(df.select("value").first()[0])
data = json.loads(df.select("value").first()[0])
# # print(data)
df = spark.createDataFrame(data)
df.show(50)

# #trades

hdfs_path = 'hdfs://localhost:9000/user/Binance_Data/lake/trades/year=2023/month=10/day=13/trades.json'

df = spark.read.json(hdfs_path, multiLine=True)
# json.dumps(df.select("value").first()[0])
data = json.loads(df.select("value").first()[0])
# # print(data)
df = spark.createDataFrame(data)
df.show(50)
row_count = df.count()
print(f"Số dòng trong DataFrame: {row_count}")