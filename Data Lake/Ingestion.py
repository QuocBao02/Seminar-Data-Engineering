# import library 
import binance
from  pyspark.sql import SparkSession
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
        self.spark=self._initSpark()
        self.partition=self._getPartitionTime()
        self.all_symbols=self._get_all_symbols()
        
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
        
    def _initSpark(self):
        ''' Initialize Spark Session
        '''
        # create spark session 
        spark=SparkSession.builder.appName("AppendDataToDataLake").getOrCreate()
        return spark
    
    def closeSpark(self):
        '''Close Spark Session'''
        self.spark.stop()
        print("Spark Session is Closed!")
    
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
    
    # get all tables data with all symbols
    # it is not used 
    def _get_all_symbols(self):
        ''' The temporary function to get all symbols '''
        all_symbols=self.binance_cli.get_all_tickers()
        df=self.spark.createDataFrame(all_symbols)
        symbols_col=df.select("symbol").distinct()
        symbol_list=[row.symbol for row in symbols_col.collect()]
        size_of_list=len(symbol_list)
        symbol_dict={i:symbol_list[i] for i in range(size_of_list)}
        return symbol_dict
    
    def getSymbolInfor(self, symbol):
        '''Get the information of a symbol and save it into datalake'''
        table='SymbolInfor'
        symbol_info = self.binance_cli.get_symbol_info(symbol=symbol)
        if self._check_emptyDF(symbol_info) == False:
            hdfs_destination=self._generatePartition(table)
            df=self.spark.createDataFrame([symbol_info])
            df.write.parquet(hdfs_destination, mode='append')        
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
        
        hdfs_destination=self._generatePartition(table)
        if self._check_emptyDF(ticker_24h) == False:
            df=self.spark.createDataFrame([ticker_24h])
            df.write.parquet(hdfs_destination, mode='append')      
            print(f"Loaded into {table} successfully!")
        pass
    
    
    def getTrades(self, symbol):
        '''Get all trades of a symbol in a day and save it into datalake'''
        table='Trades'
        hdfs_destination=self._generatePartition(table)
        
        # check the validation of fromid and lastid
        if self.firstId > 0 and self.lastId > 0:
            for id in range(self.firstId, self.lastId+1, self.step):
                trades=self.binance_cli.get_historical_trades(symbol=symbol, fromId=id, limit=1000)
                if self._check_emptyDF(trades) == False:
                    df=self.spark.createDataFrame(trades)
                    df.write.parquet(hdfs_destination, mode='append')      
                # sleep for a short time
                time.sleep(60/(self.request_per_minute+1))
            print(f"Loaded into {table} successfully!")
        pass
    
    def getKlines(self, symbol):
        '''Get the klines of all trades of a symbol in a day and save it into datalake'''
        table="Klines"
        hdfs_destination=self._generatePartition(table)
        for starttime in range(self.openTime, self.closeTime + 1, self.amount_in_12h*60*1000):
            
            klines=self.binance_cli.get_klines(symbol=symbol, interval=binance.Client.KLINE_INTERVAL_1MINUTE, startTime=starttime, limit=self.amount_in_12h)
            if self._check_emptyDF(klines) == False:
                df=self.spark.createDataFrame(klines)
                df.write.parquet(hdfs_destination, mode='append')  
            # sleep for a short time 
            time.sleep(60/(self.request_per_minute + 1))
        print(f"Loaded into {table} successfully!")
        pass    
    
    def _check_emptyDF(self, df):
        if len(df) != 0:
            return False 
        else: 
            return True

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
    binance_datalake=Binance_Ingestion_Data_Lake(api_key=api_key, secret_key=secret_key, database=Database)
    symbols=binance_datalake.all_symbols
    
    for i in range(1,50):
        symbol=symbols[i]
        print(f"\n {symbol}:")
        binance_datalake.getSymbolInfor(symbol=symbol)
        binance_datalake.getTicker_24h(symbol=symbol)
        binance_datalake.getTrades(symbol=symbol)
        binance_datalake.getKlines(symbol=symbol)
        
    binance_datalake.closeSpark()
    binance_datalake.disconnect_Binance()
    
if __name__=='__main__':
    main()
    pass