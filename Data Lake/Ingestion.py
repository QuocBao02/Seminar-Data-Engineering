# import library 
import binance
from  pyspark.sql import SparkSession
import pandas as pd
import time


class Binance_Ingestion_Data_Lake(object):
    def __init__(self, api_key=None, secret_key=None, database="Binance_Market_Data", hdfs_host="localhost", hdfs_port=9000):
        self.api_key=api_key 
        self.secret_key=secret_key 
        self.database=database
        self.hdfs_host=hdfs_host
        self.hdfs_port=hdfs_port
        self.step=1000
        self.request_per_minute=1200
        self.amount_in_12h=720
        self.binance_cli=self._connect_Binance()
        self.partition=self._getPartitionTime()
        
    def _connect_Binance(self):
        try: 
            print("Requesting with Binance Market Data")
            binance_client=binance.Client(api_key=self.api_key, api_secret=self.secret_key)
        except: 
            print("Can not connect to Binance!")
        else:
            print("Connected to Binance Successfully!")
            return binance_client
    def disconnect_Binance(self):
        '''disconnect Binance'''
        if self.binance_cli: 
            self.binance_cli.close_connection() 
        print('Binance connection is closed!')
        
    def _saveintoHDFS(self,data_source, destination):
        '''Get pandas dataframe, using spark to save into hadoop hdfs
            source: pandas dataframe
            destination: hdfs path
        '''
        # create spark session 
        spark=SparkSession.builder.appName("AppendDataToDataLake").getOrCreate()
        df=spark.createDataFrame(data_source)
        df.write.parquet(destination, mode='append')
        spark.stop()
        pass 
    
    def _getPartitionTime(self):
        '''Create directory to save data into data lake in hadoop hdfs'''
        current_time=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        year=current_time.split()[0].split('-')[0]
        month=current_time.split()[0].split('-')[1]
        day=current_time.split()[0].split('-')[2]
        partition=f'year={year}/month={month}/day={day}'
        return partition
    
    def _generatePartition(self, table):
        '''Generate the path to save into datalake'''
        hdfs_path=f"hdfs://{self.hdfs_host}:{self.hdfs_port}/{self.database}/DataLake/{table}/{self.partition}/{table}.parquet"
        return hdfs_path 
    
    
    def getSymbolInfor(self, symbol):
        '''Get the information of a symbol and save it into datalake'''
        table='SymbolInfor'
        symbol_info = self.binance_cli.get_symbol_info(symbol=symbol)
        hdfs_des=self._generatePartition(table)
        self._saveintoHDFS(source=pd.DataFrame(symbol_info, index=[0]), destination=hdfs_des)
        print(f"Loaded into {table} successfully!")
        pass
    
    def getTicker_24h(self, symbol):
        '''Get the information of Ticker in 24h of a symbol and save it into datalake'''
        table='Ticker_24h'
        ticker_24h=self.binance_cli.get_ticker(symbol=symbol)
        # get firstId and lastId 
        self.firstId=ticker_24h['firstId']
        self.lastId=ticker_24h['lastId']
        # get openTime and closeTime
        self.openTime=ticker_24h['openTime']
        self.closeTime=ticker_24h['closeTime']
        
        hdfs_des=self._generatePartition(table)
        self._saveintoHDFS(source=pd.DataFrame(ticker_24h, index=[0]), destination=hdfs_des)
        print(f"Loaded into {table} successfully!")
        pass
    
    
    def getTrades(self, symbol):
        '''Get all trades of a symbol in a day and save it into datalake'''
        table='Trades'
        hdfs_des=self._generatePartition(table)
        for id in range(self.firstId, self.lastId+1, self.step):
            trades=self.binance_cli.get_historical_trades(symbol=symbol, fromId=id, limit=1000)
            self._saveintoHDFS(source=pd.DataFrame(trades, index=[0]), destination=hdfs_des)
            # sleep for a short time
            time.sleep(60/(self.request_per_minute+1))
        print(f"Loaded into {table} successfully!")
        pass
    
    def getKlines(self, symbol):
        '''Get the klines of all trades of a symbol in a day and save it into datalake'''
        table="Klines"
        hdfs_des=self._generatePartition(table)
        for starttime in range(self.openTime, self.closeTime + 1, self.amount_in_12h*60*1000):
            klines=self.binance_cli.get_klines(symbol=symbol, interval=binance.Client.KLINE_INTERVAL_1MINUTE, startTime=starttime, limit=self.amount_in_12h)
            self._saveintoHDFS(source=pd.DataFrame(klines, index=[0]), destination=hdfs_des)
            # sleep for a short time 
            time.sleep(60/(self.request_per_minute + 1))
        print(f"Loaded into {table} successfully!")
        pass    

def get_api(path):
    '''Get api from local file'''
    with open(path, 'r') as f:
        api=f.read().split('\n')
        api_key=api[0]
        secret_key=api[1]
    return(api_key, secret_key)


    
def main():   
    api_path='/home/quocbao/MyData/Binance_API_Key/binance_api_key.txt'
    (api_key, secret_key)=get_api(path=api_path)
    Database="Binance_Market_Data"
    symbol="BTCUSDT"
    binance_datalake=Binance_Ingestion_Data_Lake(api_key=api_key, secret_key=secret_key)
    binance_datalake.getSymbolInfor(symbol=symbol)
    
    binance_datalake.disconnect_Binance()

if __name__=='__main__':
    main()
    pass