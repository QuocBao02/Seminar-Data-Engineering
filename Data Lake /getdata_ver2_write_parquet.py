from pyspark.sql import SparkSession
import os
from pyspark.sql.types import StringType
import binance
import parquet
import pandas as pd
from hdfs import InsecureClient
from datetime import datetime
import time
from pyspark.sql.types import StructType
from pyspark.sql.functions import unix_timestamp

api_key = "aRkqlapnqhNXa1bYU4Q7QWkru6DHA5sdRrmKxnRTPXjbXbZhqOPCJ8p0oNCNNbhY"
api_secret = "uK3edZV3Wy2blZHEC67UlsQVgm48JRz1WlWi5ZNrJDg4Aajt3B0QwDMQjOS6cHnH"

class Binance(object):
    def __init__(self, api=None, secret_key=None,database="Binance_Data", hdfs_host="localhost", hdfs_port=9000):
        self.api = api_key 
        self.secret_key = secret_key
        self.database=database
        self.hdfs_host = hdfs_host
        self.hdfs_port = hdfs_port
        self.metastore_port = 9084

        self.request_per_minute=4000
        self.amount_in_12h=720
        self.client = self._connect_Binace()
        self.partition=self._get_partition_time()
        self.spark=self._initSpark()
    def _initSpark(self):
        ''' Initialize spark session for ETL job '''
        
        # create spark session 
        spark = SparkSession.builder \
            .appName("Save to DataLake") \
            .config("spark.driver.memory", "6g") \
            .config("spark.executor.memory", "6g") \
            .config("spark.executor.cores", "6") \
            .config("hive.metastore.uris", f"thrift://{self.hdfs_host}:{self.metastore_port}")\
            .config("spark.sql.warehouse.dir", f"hdfs://{self.hdfs_host}:{self.hdfs_port}/user/Binance_Data/warehouse/")\
            .enableHiveSupport()\
            .getOrCreate()
        if spark:
            print("Spark Session is CREATED!")
            return spark
        else:
            print("Spark Session can not create!")

    #connet to binance.com
    def _connect_Binace(self):
        print('Requesting with Binance Market Data!')
        client = binance.Client(self.api, self.secret_key)
        return client 
    
    def closeSpark(self):
        '''Close Spark Session'''
        self.spark.stop()
        print("Spark Session is Closed!")

    def _get_partition_time(self):
        '''Create directory to save data into data lake in hadoop hdfs'''
        # Get the current date and time
        now = datetime.now()
        partition=f'year={now.year}/month={now.month}/day={now.day}'
        return partition
    
    def _generate_partition(self, table):
        '''Generate the path to save into datalake'''
        hdfs_path=f"hdfs://{self.hdfs_host}:{self.hdfs_port}/user/{self.database}/lake/{table}/{self.partition}/{table}.parquet"
        return hdfs_path 
    
    def _write_file_hdfs(self,data, destination):
        '''Get pandas dataframe, using spark to save into hadoop hdfs
            source: pandas dataframe
            destination: hdfs path
        '''
        
        try:
            # Tạo DataFrame từ chuỗi parquet
            df = self.spark.createDataFrame(data)

            # Lưu DataFrame xuống HDFS dưới dạng tệp parquet
            df.write.mode("overwrite").parquet(destination)

            print(f"Save to DataLake with {destination}")
        except Exception as e:
            print(f"Error: {str(e)}")
        # self.closeSpark()

        pass 

    def _get_symbol_list(self):
        # Get all the information symbols
        rq_symbols = self.client.get_all_tickers()
        # create list
        symbols=[]
        count=0
        for symbol in rq_symbols:
            count+=1
            symbols.append(symbol['symbol'])#append to list
            if count ==100:
                break
        print(count)
        return symbols
    
    def get_symbol_infor(self,symbols):
        '''Get the information of a symbol and save it into datalake'''
        table='symbol_infor'
        data = []
        hdfs_des=self._generate_partition(table)
        for symbol in symbols:
            df= self.client.get_symbol_info(symbol=symbol)
            if df != None:
                data.append(df)

        self._write_file_hdfs(data = data, destination = hdfs_des)
        print(f"Loaded into {table} successfully!")
        pass
    
    def _getLastestIdDataFrame(self, old_table, column_name=None):
        '''Get the latest id of column from table'''
        try:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database};")
            self.spark.sql(f"USE {self.database};")
            latestDF_value = self.spark.sql(f"SELECT symbol_id, MAX({column_name}) as {column_name} FROM {old_table} GROUP BY symbol_id;")
            
            # Kiểm tra xem DataFrame có trống hay không
            if not latestDF_value.isEmpty():
                return latestDF_value
            else:
                # Tạo DataFrame rỗng với schema trống
                empty_schema = StructType([])
                return self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema=empty_schema)
        except Exception as e:
            # print(f"Error: {str(e)}")
            # Tạo DataFrame rỗng với schema trống nếu có lỗi
            empty_schema = StructType([])
        return self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema=empty_schema)

    def get_ticker_24h(self, symbols):
        '''Get the information of Ticker in 24h of a symbol and save it into datalake'''
        table = 'ticker_24h'
        
        # Lấy dữ liệu mới nhất từ DataFrame
        old_data_trade =self._getLastestIdDataFrame(old_table="trades", column_name="trade_id")
        old_data_trade_dir = {row["symbol_id"]: row["trade_id"] for row in old_data_trade.collect()}
        
        # Lấy dữ liệu mới nhất từ DataFrame
        old_data_kline = self._getLastestIdDataFrame(old_table="klines", column_name="closeTime")
        if not old_data_kline.isEmpty():
            datetime_format = "yyyy-MM-dd HH:mm:ss"
            old_data_kline = old_data_kline.withColumn("closeTime", unix_timestamp(old_data_kline["closeTime"], datetime_format)*1000)
        old_data_kline_dir = {row["symbol_id"]: row["closeTime"] for row in old_data_kline.collect()}
        # print(old_data_kline_dir)
        
        data = []
        symbols_list = []

        for symbol in symbols:
            # Ticker_24h sẽ chứa thông tin mới nhất nếu có, nếu không thì lấy từ API
            ticker_24h = self.client.get_ticker(symbol=symbol)

            # Lấy trade_id từ dữ liệu cũ hoặc từ API
            last_trade_id = old_data_trade_dir.get(symbol, ticker_24h["firstId"])
            last_kline_id = old_data_kline_dir.get(symbol, ticker_24h["openTime"])
            # Tạo dictionary chứa thông tin symbol
            symbol_data = {
                "symbol": ticker_24h["symbol"],
                "firstId": last_trade_id,
                "lastId": ticker_24h["lastId"],
                "openTime": last_kline_id,
                "closeTime": ticker_24h["closeTime"]
            }

            data.append(ticker_24h)
            symbols_list.append(symbol_data)

        hdfs_des = self._generate_partition(table)
        self._write_file_hdfs(data=data, destination=hdfs_des)
        print(f"Loaded into {table} successfully!")
        return symbols_list



    def get_trades(self, symbols_list):
        '''Get all trades of a symbol in a day and save it into datalake'''
        table='trades'
        data = []
        count=0
        hdfs_des=self._generate_partition(table)
        for symbol in symbols_list:
            count+=1
            # time.sleep(0.01)
            symbol_name = symbol["symbol"]
            for current_id in range(symbol["firstId"], symbol["lastId"] - 1, 1000):
                print(f"Symbol: {symbol_name}, Current ID: {current_id}")
                trades = self.client.get_historical_trades(symbol = symbol_name, fromId=current_id, limit=1000)
                for trade in trades:
                    trade["symbol"] = symbol_name
                    data.append(trade)
            if count % 200==0:
                    self._write_file_hdfs(data = data, destination = hdfs_des)
                    data = []        
        self._write_file_hdfs(data = data, destination = hdfs_des)
        print(f"Loaded into {table} successfully!")

    def get_klines(self, symbols_list):
        '''Get the klines of all trades of a symbol in a day and save it into datalake'''
        table = "klines"
        data = []
        count=0
        hdfs_des = self._generate_partition(table)
        for symbol in symbols_list:
            symbol_name = symbol["symbol"]
            for starttime in range(symbol["openTime"], symbol["closeTime"] + 1, self.amount_in_12h * 60 * 1000):
                klines = self.client.get_klines(symbol=symbol["symbol"], interval=binance.Client.KLINE_INTERVAL_1MINUTE, startTime=starttime, limit=self.amount_in_12h)
                count+=1
                # Chuyển danh sách Klines thành danh sách các từ điển
                for kline in klines:
                    kline_dict = {
                        "symbol": symbol_name,
                        "openTime": kline[0],
                        "open": kline[1],
                        "high": kline[2],
                        "low": kline[3],
                        "close": kline[4],
                        "volume": kline[5],
                        "closeTime": kline[6],
                        "quoteAssetVolume": kline[7],
                        "trades": kline[8],
                        "takerBuyBaseAssetVolume": kline[9],
                        "takerBuyQuoteAssetVolume": kline[10],
                        "ignore": kline[11]
                    }
                    data.append(kline_dict)
                print(f"Symbol: {symbol_name}, starttime ID: {starttime}")
            # if count % 100==0:
            #     self._write_file_hdfs(data = data, destination = hdfs_des)
            #     data = []
            # Sleep for a short time 
            # time.sleep(60 / (self.request_per_minute + 1))
        
        self._write_file_hdfs(data=data, destination=hdfs_des)
        print(f"Loaded into {table} successfully!")



if __name__=='__main__':
    _binance = Binance( api=api_key, secret_key=api_secret)

    #symbol_list
    symbols=_binance._get_symbol_list()
    # print(symbols)

    #symbol_info
    # symbols=['KSMBTC', 'KSMUSDT', 'EGLDBNB', 'EGLDBTC']
    # symbols=['BELETH']
    _binance.get_symbol_infor(symbols=symbols)

    #ticker_24h
    symbols_list=_binance.get_ticker_24h(symbols=symbols)
    print(symbols_list)
    # print(_binance.client.get_symbol_info(symbol="BELETH"))

    # trade
    _binance.get_trades(symbols_list=symbols_list)

    #klines
    _binance.get_klines(symbols_list=symbols_list)
    # df=_binance._getLastestIdDataFrame(old_table="trades", column_name="trade_id")
    # if df==0:
    #         old_data_dir ={"symbol_id":"","trade_id":-1}
    #         print(old_data_dir)
    # else:
    #         old_data_dir = {row["symbol_id"]: row["trade_id"] for row in df.collect()}
    #         print(old_data_dir)
    # _binance.closeSpark()
    # _binance.closeSpark()
    # _binance.closeSpark()
    _binance.closeSpark()
    