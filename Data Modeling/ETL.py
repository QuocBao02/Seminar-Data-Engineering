from datetime import datetime
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import SparkSession 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
from pyspark.sql.functions import col, expr, lit
# spark = SparkSession.builder.appName("PySparkTest")\
#     .config("hive.exec.dynamic.partition.mode", "nonstrict")\
#     .config("hive.exec.dynamic.partition", "true")\
#     .config("spark.hadoop.hive.exec.dynamic.partition.mode", "non-strict")\
#     .config("spark.hadoop.hive.exec.dynamic.partition", "true")\
#     .config("hive.metastore.uris", "thrift://localhost:9084")\
#     .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/Binance_Data/warehouse/")\
#     .enableHiveSupport()\
#     .getOrCreate()
class PathGenerator:
    def __init__(self, hdfs_host="localhost", hdfs_port=9000, database = "Binance_Data"):
        self.hdfs_host = hdfs_host
        self.hdfs_port = hdfs_port
        self.database = database
        self.partition=self.get_partition_time()

    def get_partition_time(self):
        '''Create directory to save data into data lake in Hadoop HDFS'''
        # Get the current date and time
        now = datetime.now()
        partition = f'year={now.year}/month={now.month}/day={now.day}'
        return partition

    def get_generate_partition(self, table):
        '''Generate the path to save into data lake'''
        partition = self.get_partition_time()
        hdfs_path = f"hdfs://{self.hdfs_host}:{self.hdfs_port}/user/{self.database}/lake/{table}/{self.partition}/{table}.parquet"
        return hdfs_path
    
class ETLJob:
    def __init__(self, database, host, hdfs_port, metastore_port):
        self.database=database
        self.host=host
        self.hdfs_port=hdfs_port
        self.metastore_port=metastore_port
        self.spark=self._initSpark()
    def _initSpark(self):
        ''' Initialize spark session for ETL job '''
        
        spark=SparkSession.builder\
            .appName("InitSparkSessionForETL")\
            .config("hive.metastore.uris", f"thrift://{self.host}:{self.metastore_port}")\
            .config("spark.sql.warehouse.dir", f"hdfs://{self.host}:{self.hdfs_port}/user/Binance_Data/warehouse/")\
            .enableHiveSupport()\
            .getOrCreate()
        if spark:
            print("Spark Session is CREATED!")
            return spark
        else:
            print("Spark Session can not create!")
        

    def extract_data(self, input_path):
        return self.spark.read.parquet(input_path, multiLine=True)

    def transform_data(self, input_df, transformation_logic):
        # Thực hiện các biến đổi dữ liệu tại đây bằng cách sử dụng transformation_logic cụ thể cho từng bảng
        transformed_df = transformation_logic(self, input_df)
        return transformed_df
 
    def load_data(self, db_name, table_name, output_df):
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name};")
        self.spark.sql(f"USE {db_name};")
        # output_df.show()
        output_df.write.partitionBy('symbol_id').mode("append").saveAsTable(f"{self.database}.{table_name}")


    def run(self, input_path, db_name, table_name, transformation_logic):
        input_data = self.extract_data(input_path)
        transformed_data = self.transform_data(input_data, transformation_logic)
        self.load_data(db_name, table_name, transformed_data)
        # self.spark.stop()

    # def _getLastestId(self, old_table, column_name=None):
    #     '''Get the latest id of column from table'''
    #     self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database};")
    #     self.spark.sql(f"USE {self.database};")
    #     latestDF_value = self.spark.sql(f"SELECT symbol_id, MAX({column_name}) as {column_name} FROM {old_table} GROUP BY symbol_id;")
    #     # latestDF_value.show()
    #     if latestDF_value.isEmpty():
    #         print(f"Error, getLastestId table ={old_table}")
    #         return 0
    #     else: 
    #         return latestDF_value

    def _getLastestIdDataFrame(self, old_table, column_name=None):
        '''Get the latest id of column from table'''
        try:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database};")
            self.spark.sql(f"USE {self.database};")
            latestDF_value = self.spark.sql(f"SELECT symbol_id, MAX({column_name}) as {column_name} FROM {old_table} GROUP BY symbol_id;")
            if latestDF_value.isEmpty():
                print(f"Error, getLastestId table ={old_table}")
                return 0
            else: 
                return latestDF_value
        except :
            print(f"Error")
            return 0
        
    def _getLastestIdTable(self, old_table, column_name=None):
        '''Get the latest id of column from table'''
        try:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database};")
            self.spark.sql(f"USE {self.database};")
            latestDF_value=self.spark.sql(f"SELECT {column_name} FROM {old_table} ORDER BY {column_name} DESC LIMIT 1;")
            # latestDF_value.show()
            # print(latestDF_value.collect())
            if latestDF_value.isEmpty():
                return 0
            else: 
                latest_value=latestDF_value.collect()[0][f'{column_name}']
                # print(latest_value)
                return latest_value
        except:
            return 0
    
def getDataFromDataLake(self, table):
        ''' Using spark to read from hdfs
        param: Name of table
        return: spark dataframe
        '''
        path = PathGenerator(hdfs_host="localhost", hdfs_port=9000,database = "Binance_Data")
        partition=path.get_partition_time(table)
        hdfs_path=f"hdfs://{self.host}:{self.hdfs_port}/{self.database}/lake/{table}//{partition}/{table}.parquet"
        try:
            raw_data=self.spark.read.parquet(hdfs_path)
        except:
            print(f"{table} is empty dataframe!")
            return 0
        
        return raw_data
  


# Định nghĩa các hàm biến đổi riêng cho từng bảng
def transform_table_symbol_infor(self, input_df):
    # Logic biến đổi cho bảng 1
    latest_filter_type_id_df = self._getLastestIdDataFrame(old_table="symbol_infor",column_name="filterstype_id")
    if(latest_filter_type_id_df==0):
        data=input_df.withColumn("filterstype_id", lit(0))
    
    else:
        data = input_df.join(
            latest_filter_type_id_df,
            input_df.symbol == latest_filter_type_id_df.symbol_id,
            "left_outer"
        )
    # data.show()
    # data.filter(data.filterstype_id == 1).show()
    # row_count = data.count()
    # print(f"Số dòng trong DataFrame: {row_count}")
    data.createOrReplaceTempView("symbol_infor_temp_tb")
    
    transformed=self.spark.sql(f"""
    SELECT symbol as symbol_id, \
            allowTrailingStop ,\
            baseAsset ,\
            baseAssetPrecision ,\
            baseCommissionPrecision ,\
            cancelReplaceAllowed ,\
            defaultSelfTradePreventionMode ,\
            COALESCE(filterstype_id, 0) + 1 AS filterstype_id,\
            icebergAllowed ,\
            isMarginTradingAllowed ,\
            isSpotTradingAllowed ,\
            ocoAllowed ,\
            quoteAsset ,\
            quoteAssetPrecision ,\
            quoteCommissionPrecision ,\
            quoteOrderQtyMarketAllowed ,\
            quotePrecision ,\
            status 
        FROM symbol_infor_temp_tb;
    """)
    symbol_sql=self.spark.sql("""SELECT symbol, filterstype_id +1 AS filterstype_id, filters  from symbol_infor_temp_tb;""").withColumnRenamed('symbol', 'symbol_id')
    # filter_types = ["PRICE_FILTER", "LOT_SIZE","ICEBERG_PARTS", "TRAILING_DELTA","PERCENT_PRICE_BY_SIDE", "NOTIONAL", "MAX_NUM_ORDERS", "MAX_NUM_ALGO_ORDERS"]
                        
    PRICE_FILTER_df = symbol_sql.withColumn('minPrice', expr("filters[0].minPrice"))\
                            .withColumn('maxPrice', expr("filters[0].maxPrice"))\
                            .withColumn('tickSize', expr("filters[0].tickSize"))\
                            .select("symbol_id", "filterstype_id","minPrice","maxPrice", "tickSize")
    # PRICE_FILTER_df.show()
    PRICE_FILTER_df.write.partitionBy(['symbol_id', 'filterstype_id']).mode("append").format("parquet").saveAsTable(f"{self.database}.price_filter_detail")

    LOT_SIZE_df = symbol_sql.withColumn('minQty', expr("filters[1].minQty"))\
                            .withColumn('maxQty', expr("filters[1].maxQty"))\
                            .withColumn('stepSize', expr("filters[1].stepSize"))\
                            .select("symbol_id", "filterstype_id","minQty","maxQty", "stepSize")
    # LOT_SIZE_df.show()
    LOT_SIZE_df.write.partitionBy(['symbol_id', 'filterstype_id']).mode("append").format("parquet").saveAsTable(f"{self.database}.lot_size_detail")
    
    ICEBERG_PARTS_df = symbol_sql.withColumn('limit', expr("filters[2].limit"))\
                            .select("symbol_id", "filterstype_id","limit")
    # ICEBERG_PARTS_df.show()
    ICEBERG_PARTS_df.write.partitionBy(['symbol_id', 'filterstype_id']).mode("append").format("parquet").saveAsTable(f"{self.database}.iceberg_parts_detail")
    
    MARKET_LOT_SIZE_df = symbol_sql.withColumn('minQty', expr("filters[3].minQty"))\
                            .withColumn('maxQty', expr("filters[3].maxQty"))\
                            .withColumn('stepSize', expr("filters[3].stepSize"))\
                            .select("symbol_id", "filterstype_id","minQty","maxQty", "stepSize")
    # MARKET_LOT_SIZE_df.show()
    MARKET_LOT_SIZE_df.write.partitionBy(['symbol_id', 'filterstype_id']).mode("append").format("parquet").saveAsTable(f"{self.database}.market_lot_size_deltal")
    
    TRAILING_DELTA_df = symbol_sql.withColumn('minTrailingAboveDelta', expr("filters[4].minTrailingAboveDelta"))\
                            .withColumn('maxTrailingAboveDelta', expr("filters[4].maxTrailingAboveDelta"))\
                            .withColumn('minTrailingBelowDelta', expr("filters[4].minTrailingBelowDelta"))\
                            .withColumn('maxTrailingBelowDelta', expr("filters[4].maxTrailingBelowDelta"))\
                            .select("symbol_id", "filterstype_id","minTrailingAboveDelta","maxTrailingAboveDelta", "minTrailingBelowDelta","maxTrailingBelowDelta")
    # TRAILING_DELTA_df.show()
    TRAILING_DELTA_df.write.partitionBy(['symbol_id', 'filterstype_id']).mode("append").format("parquet").saveAsTable(f"{self.database}.trailing_delta")
    
    PERCENT_PRICE_BY_SIDE_df = symbol_sql.withColumn('bidMultiplierUp', expr("filters[5].bidMultiplierUp"))\
                            .withColumn('bidMultiplierDown', expr("filters[5].bidMultiplierDown"))\
                            .withColumn('askMultiplierUp', expr("filters[5].askMultiplierUp"))\
                            .withColumn('askMultiplierDown', expr("filters[5].askMultiplierDown"))\
                            .withColumn('avgPriceMins', expr("filters[5].avgPriceMins"))\
                            .select("symbol_id", "filterstype_id","bidMultiplierUp","bidMultiplierDown", "askMultiplierUp", "askMultiplierDown","avgPriceMins")
    # PERCENT_PRICE_BY_SIDE_df.show()
    PERCENT_PRICE_BY_SIDE_df.write.partitionBy(['symbol_id', 'filterstype_id']).mode("append").format("parquet").saveAsTable(f"{self.database}.percent_price_by_side_detail")
    
    NOTIONAL_df = symbol_sql.withColumn('minNotional', expr("filters[6].minNotional"))\
                            .withColumn('applyMinToMarket', expr("filters[6].applyMinToMarket"))\
                            .withColumn('maxNotional', expr("filters[6].maxNotional"))\
                            .withColumn('applyMaxToMarket', expr("filters[6].applyMaxToMarket"))\
                            .withColumn('avgPriceMins', expr("filters[6].avgPriceMins"))\
                            .select("symbol_id", "filterstype_id","minNotional","applyMinToMarket", "maxNotional", "applyMaxToMarket", "avgPriceMins")
    # NOTIONAL_df.show()
    NOTIONAL_df.write.partitionBy(['symbol_id', 'filterstype_id']).mode("append").format("parquet").saveAsTable(f"{self.database}.notional_detail")

    MAX_NUM_ORDERS_df = symbol_sql.withColumn('maxNumOrders', expr("filters[7].maxNumOrders"))\
                            .select("symbol_id", "filterstype_id","maxNumOrders")
    # MAX_NUM_ORDERS_df.show()
    MAX_NUM_ORDERS_df.write.partitionBy(['symbol_id', 'filterstype_id']).mode("append").format("parquet").saveAsTable(f"{self.database}.max_num_orders_detail")
    
    MAX_NUM_ALGO_ORDERS_df = symbol_sql.withColumn('maxNumAlgoOrders', expr("filters[8].maxNumAlgoOrders"))\
                            .select("symbol_id", "filterstype_id","maxNumAlgoOrders")
    # MAX_NUM_ALGO_ORDERS_df.show()
    MAX_NUM_ALGO_ORDERS_df.write.partitionBy(['symbol_id', 'filterstype_id']).mode("append").format("parquet").saveAsTable(f"{self.database}.max_num_algo_orders_detail")

    return transformed
def transform_table_trades(self, input_df):
    # Logic biến đổi cho bảng 1
    input_df.createOrReplaceTempView("raw_data")
    transformed=self.spark.sql("""
    SELECT id as trade_id, 
            symbol as symbol_id, 
            isBestMatch,
            isBuyerMaker, 
            price, 
            qty,
            quoteQty, 
            FROM_UNIXTIME(time / 1000) AS timestamp_column
    FROM raw_data;
    """)
    return transformed

def transform_table_klines(self, input_df):
    # Logic biến đổi cho bảng 1
    input_df.createOrReplaceTempView("klines_temp_tb")
    # input_df.show()
    latest_kline_id = self._getLastestIdTable(old_table = "klines", column_name="kline_id")
    transformed=self.spark.sql(f"""
    SELECT symbol as symbol_id, \
    ROW_NUMBER() OVER (ORDER BY symbol) + {latest_kline_id} as kline_id, \
    FROM_UNIXTIME(openTime/1000) AS opentime, \
    FROM_UNIXTIME(closeTime/1000) AS closeTime, \
    high,\
    ignore,\
    low,\
    open,\
    close,\
    quoteAssetVolume,\
    takerBuyBaseAssetVolume,\
    takerBuyQuoteAssetVolume,\
    trades,\
    volume \
    FROM klines_temp_tb;
    """)
    return transformed

def transform_table_ticker24h(self, input_df):
    # Logic biến đổi cho bảng 2

    input_df.createOrReplaceTempView("ticker_24h_tb")
    # latest_ticker_id=self._getLastestId(old_table="Ticker_24h", symbol_id=symbol_id, column_name='ticker_id') 
    latest_ticker_id = self._getLastestIdTable(old_table = "ticker_24h", column_name="ticker_id")
    transformed=self.spark.sql(f"""
    SELECT symbol as symbol_id,  \
                ROW_NUMBER() OVER (ORDER BY symbol) + {latest_ticker_id} as ticker_id, \
                priceChange ,\
                priceChangePercent ,\
                weightedAvgPrice ,\
                prevClosePrice ,\
                lastPrice ,\
                bidPrice ,\
                askPrice ,\
                openPrice ,\
                highPrice ,\
                lowPrice ,\
                volume ,\
                FROM_UNIXTIME(openTime/1000) AS openTime ,\
                FROM_UNIXTIME(closeTime/1000) AS closeTime ,\
                firstId AS fristTradeId, \
                lastId AS lastTradeId, \
                count\
            FROM ticker_24h_tb;
    """)
    return transformed

if __name__ == "__main__":
    metastore_uri = "thrift://localhost:9084"
    warehouse_dir = "hdfs://localhost:9000/user/Binance_Data/warehouse/"

    hdfs_host="localhost"
    hdfs_port=9000
    metastore_port=9084
    database_name = "Binance_Data"
    table_name = "trades"

    etl_job = ETLJob(database_name, hdfs_host, hdfs_port, metastore_port) 
    path = PathGenerator(hdfs_host="localhost", hdfs_port=9000,database = "Binance_Data")
    
    # ETL cho bảng 2
    table_name = "symbol_infor"
    input_path = path.get_generate_partition(table_name)
    etl_job.run(input_path, database_name, table_name, transform_table_symbol_infor)

    # ETL cho bảng 2
    table_name = "ticker_24h"
    input_path = path.get_generate_partition(table_name)
    etl_job.run(input_path, database_name, table_name, transform_table_ticker24h)

    # ETL cho bảng 1
    table_name = "trades"
    input_path = path.get_generate_partition(table_name)
    etl_job.run(input_path, database_name, table_name, transform_table_trades)

    # ETL cho bảng 2
    table_name = "klines"
    input_path = path.get_generate_partition(table_name)
    etl_job.run(input_path, database_name, table_name, transform_table_klines)

