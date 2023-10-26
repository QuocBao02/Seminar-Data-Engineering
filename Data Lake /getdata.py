from pyspark.sql import SparkSession
import os
from pyspark.sql.types import StringType
import binance
import json
import pandas as pd
from hdfs import InsecureClient
from datetime import datetime
import time

api_key = "aRkqlapnqhNXa1bYU4Q7QWkru6DHA5sdRrmKxnRTPXjbXbZhqOPCJ8p0oNCNNbhY"
api_secret = "uK3edZV3Wy2blZHEC67UlsQVgm48JRz1WlWi5ZNrJDg4Aajt3B0QwDMQjOS6cHnH"

class Binance(object):
    def __init__(self, api=None, secret_key=None,database="Binance_Data", hdfs_host="localhost", hdfs_port=9000):
        self.api = api_key 
        self.secret_key = secret_key
        self.database=database
        self.hdfs_host = hdfs_host
        self.hdfs_port = hdfs_port

        self.request_per_minute=1500
        self.amount_in_12h=720
        self.client = self._connect_Binace()
        self.partition=self._get_partition_time()
    
    #connet to binance.com
    def _connect_Binace(self):
        print('Requesting with Binance Market Data!')
        client = binance.Client(self.api, self.secret_key,testnet=True)
        return client 
    
    def _get_partition_time(self):
        '''Create directory to save data into data lake in hadoop hdfs'''
        # Get the current date and time
        now = datetime.now()
        partition=f'year={now.year}/month={now.month}/day={now.day}'
        return partition
    
    def _generate_partition(self, table):
        '''Generate the path to save into datalake'''
        hdfs_path=f"hdfs://{self.hdfs_host}:{self.hdfs_port}/user/{self.database}/lake/{table}/{self.partition}/{table}.json"
        return hdfs_path 
    
    def _write_file_hdfs(self,data, destination):
        '''Get pandas dataframe, using spark to save into hadoop hdfs
            source: pandas dataframe
            destination: hdfs path
        '''
        # create spark session 
        # spark=SparkSession.builder.appName("Save to DataLake").getOrCreate()
        spark = SparkSession.builder \
            .appName("Save to DataLake") \
            .config("spark.driver.memory", "6g") \
            .config("spark.executor.memory", "3g") \
            .config("spark.executor.cores", "4") \
            .getOrCreate()
        try:
            # Chuyển dữ liệu thành chuỗi JSON
            json_data = json.dumps(data)

            # Tạo DataFrame từ chuỗi JSON
            df = spark.createDataFrame([json_data], StringType())

            # Lưu DataFrame xuống HDFS dưới dạng tệp JSON
            df.write.mode("overwrite").json(destination)

            print(f"Save to DataLake with {destination}")
        except Exception as e:
            print(f"Error: {str(e)}")
        spark.stop()
        pass 

    def _get_symbol_list(self):
        # Get all the information symbols
        rq_symbols = self.client.get_all_tickers()
        # create list
        symbols=[]
        for symbol in rq_symbols:
            symbols.append(symbol['symbol'])#append to list
        return symbols
    
    def get_symbol_infor(self,symbols):
        '''Get the information of a symbol and save it into datalake'''
        table='symbol_infor'
        data = []
        for symbol in symbols:
            time.sleep(0.1)
            data.append(self.client.get_symbol_info(symbol=symbol))
        hdfs_des=self._generate_partition(table)
        # print(data)
        self._write_file_hdfs(data = data, destination = hdfs_des)
        print(f"Loaded into {table} successfully!")
        pass
    
    def get_ticker_24h(self, symbols):
        '''Get the information of Ticker in 24h of a symbol and save it into datalake'''
        table='ticker_24h'
        symbols_list = []
        data = []
        for symbol in symbols:
            time.sleep(0.1)
            ticker_24h=self.client.get_ticker(symbol=symbol)
            symbol_data = {
                "symbol" : ticker_24h["symbol"],
                "firstId" : ticker_24h["firstId"],
                "lastId" : ticker_24h["lastId"],
                "openTime" : ticker_24h["openTime"], 
                "closeTime" : ticker_24h["closeTime"]
            }
            data.append(ticker_24h)
            symbols_list.append(symbol_data)
        # print(symbols_list)
        # print(data)
        hdfs_des=self._generate_partition(table)
        self._write_file_hdfs(data = data, destination = hdfs_des)
        print(f"Loaded into {table} successfully!")
        return symbols_list

    def get_trades(self, symbols_list):
        '''Get all trades of a symbol in a day and save it into datalake'''
        table='trades'
        data = []
        for symbol in symbols_list:
            time.sleep(0.1)
            symbol_name = symbol["symbol"]
            for current_id in range(symbol["firstId"], symbol["lastId"] - 1, 1000):
                # print(f"Symbol: {symbol_name}, Current ID: {current_id}")
                trades = self.client.get_historical_trades(symbol = symbol_name, fromId=current_id, limit=1000)
                trades_data = {
                    "symbol" : symbol_name,
                    "trade" : trades
                }
                data.append(trades_data)
        # print(data)
        hdfs_des=self._generate_partition(table)
        self._write_file_hdfs(data = data, destination = hdfs_des)
        print(f"Loaded into {table} successfully!")


    def get_klines(self, symbols_list):
        '''Get the klines of all trades of a symbol in a day and save it into datalake'''
        table = "klines"
        data = []
        
        for symbol in symbols_list:
            # time.sleep(0.2)
            for starttime in range(symbol["openTime"], symbol["closeTime"] + 1, self.amount_in_12h*60*1000):
                klines = self.client.get_klines(symbol = symbol["symbol"], interval=binance.Client.KLINE_INTERVAL_1MINUTE, startTime=starttime, limit=self.amount_in_12h)
                klines_data = {
                    "symbol" : symbol["symbol"],
                    "trade" : klines
                }
                data.append(klines_data)
                # sleep for a short time 
                time.sleep(60/(self.request_per_minute + 1))

        hdfs_des = self._generate_partition(table)
        # print(data)
        self._write_file_hdfs(data = data, destination = hdfs_des)
        print(f"Loaded into {table} successfully!")



if __name__=='__main__':
    _binance = Binance( api=api_key, secret_key=api_secret)

    #symbol_list
    symbols=_binance._get_symbol_list()
    # print(symbols)

    #symbol_info
    _binance.get_symbol_infor(symbols=symbols)

    #ticker_24h
    symbols_list=_binance.get_ticker_24h(symbols=symbols)
    # print(symbols_list)

    # trade
    _binance.get_trades(symbols_list=symbols_list)

    #klines
    _binance.get_klines(symbols_list=symbols_list)