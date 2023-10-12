from pyspark.sql import SparkSession
import os
from pyspark.sql.types import StringType
import binance
import json
import pandas as pd
# import pyarrow as pa
# import pyarrow.parquet as pq
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

        self.request_per_minute=1200
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
        spark=SparkSession.builder.appName("Save to DataLake").getOrCreate()
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
            time.sleep(0.5)
            data.append(self.client.get_symbol_info(symbol=symbol))
        hdfs_des=self._generate_partition(table)
        print(data)
        self._write_file_hdfs(data = data, destination = hdfs_des)
        print(f"Loaded into {table} successfully!")
        pass
    
    def get_ticker_24h(self, symbol):
        '''Get the information of Ticker in 24h of a symbol and save it into datalake'''
        table='Ticker_24h'
        ticker_24h=self.client.get_ticker(symbol=symbol)
        # get firstId and lastId 
        self.firstId=ticker_24h['firstId']
        self.lastId=ticker_24h['lastId']
        # get openTime and closeTime
        self.openTime=ticker_24h['openTime']
        self.closeTime=ticker_24h['closeTime']
        
        hdfs_des=self._generatePartition(table)
        self._saveintoHDFS(data_source=ticker_24h, destination=hdfs_des)
        print(f"Loaded into {table} successfully!")
        pass

    def get_historical_trades(self, **params):
        trades = self.client.get_historical_trades()
        return trades

    def get_klines(self, **params):
        klines = self.client.get_klines()
        return klines 



if __name__=='__main__':
    _binance = Binance( api=api_key, secret_key=api_secret)
    # ticker_id = 1
    # kline_id = 1
    # symbol = "BTCUSDT" 

    # #tickers = _binance.get_all_tickers()
    # #print(tickers)
    # #_binance.write_file(tickers,"tickers")
    # symbols=_binance.get_symbols()
    # spark_data= pd.DataFrame(symbols)
    # print(spark_data.symbol.tail())
  

    # for symbol in _binance._get_symbol_list():
    #     symbol_info=_binance.client.get_ticker(symbol="ONEBUSD")
    #     print(symbol_info)
    # pass
    # print(_binance.client.get_historical_trades(symbol="ONEBUSD", fromId=470, limit=1000))

    # (firstId, lastId), (openTime, closeTime) = _binance.insert_Ticker_24h(ticker_id=ticker_id, symbol_id=symbol_id)
    # _binance.insert_Trades(start_trade_id=firstId, end_trade_id=lastId, symbol_id=symbol_id) 
    # # print((firstId, lastId))
    # _binance.insert_Klines(openTime=openTime, closeTime=closeTime, kline_id=kline_id, symbol_id = symbol_id)
    # print(len(_binance.client.get_klines(symbol = symbol, startTime=openTime, interval=binance.Client.KLINE_INTERVAL_1MINUTE)))
    # print((firstId, lastId), (openTime, closeTime))
    # _binance.disconnect_Binance()
    # _binance.disconnect_Mysql_server()


    symbols=_binance._get_symbol_list()
    # print(symbols)
    _binance.get_symbol_infor(symbols=symbols)

    # students = {
    #     "student1": {
    #         "name": "John",
    #         "yearofbirth": 2000,
    #         "class": "12A1"
    #     },
    #     "student2": {
    #         "name": "Kane",
    #         "yearofbirth": 2002,
    #         "class": "11B1"
    #     },
    #     "student3":{
    #         "name": "Son",
    #         "yearofbirth": 2001,
    #         "class": "12A2"
    #     }
    #     }
