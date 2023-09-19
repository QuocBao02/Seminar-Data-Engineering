# import library 
import binance
from  pyspark.sql import SparkSession, Row
import pandas as pd
import hdfs 
import time


# get api from local file
def get_api(path):
    with open(path, 'r') as f:
        api=f.read().split('\n')
        api_key=api[0]
        secret_key=api[1]
    return(api_key, secret_key)
        
api_path='/home/quocbao/MyData/Binance_API_Key/binance_api_key.txt'
(api_key, secret_key)=get_api(path=api_path)

# connect to binance and get data
binance_client=binance.Client(api_key=api_key, api_secret=secret_key)
if binance_client is not None:
    print("Connected to Binance Successfully!")

# connect to hadoop hdfs 
    # configuration 
# hdfs_host='localhost'
# hdfs_port=9870
# hdfs_cli= hdfs.InsecureClient(f'http://{hdfs_host}:{hdfs_port}', user='hadoop')
# if hdfs_cli is not None:
#     print("Connected to Hadoop HDFS Successfully!")
    
# create directory to save data into data lake in hadoop hdfs
 # get time at the moment 
current_time=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
year=current_time.split()[0].split('-')[0]
month=current_time.split()[0].split('-')[1]
day=current_time.split()[0].split('-')[2]
partition=f'year={year}/month={month}/day={day}'
Database="Binance_Market_Data"


# change type of data 
symbol="BTCUSDT"

# get ticker 24h data
ticker_24h=binance_client.get_ticker(symbol=symbol)


# create spark session 
spark=SparkSession.builder.appName("AppendDataToDataLake").getOrCreate()
# create ticker_24h data frame 
print(ticker_24h)
# ticker_24h_df=spark.createDataFrame(ticker_24h)
ticker_24h_df=pd.DataFrame(ticker_24h, index=[0])
sp_df=spark.createDataFrame(ticker_24h_df)

# save to hdfs 
hdfs_path=f"hdfs://localhost:9000/{Database}/Ticker_24h/{partition}/ticker_24h.parquet"
sp_df.write.parquet(hdfs_path, mode='overwrite')
spark.stop()


# Define the HDFS path where you want to save the Parquet file
# Write the Parquet file to HDFS






# get firstId and lastId 
firstId=ticker_24h['firstId']
lastId=ticker_24h['lastId']
# get openTime and closeTime
openTime=ticker_24h['openTime']
closeTime=ticker_24h['closeTime']
# get symbol information 
symbol_infor=binance_client.get_symbol_info(symbol=symbol)
# get trades 
step=1000
request_per_minute=1200
# for id in range(firstId, lastId+1, step):
    # trades=binance_client.get_historical_trades(symbol=symbol, fromId=id, limit=1000)
    # to do something. 
    
    
    
    # sleep for a short time
    # time.sleep(60/(request_per_minute+1))

# get kline
amount_in_12h=720
# for starttime in range(openTime, closeTime + 1, amount_in_12h*60*1000):
    # klines=binance_client.get_klines(symbol=symbol, interval=binance.Client.KLINE_INTERVAL_1MINUTE, startTime=starttime, limit=amount_in_12h)
    # to do something
    
    # sleep for a short time 
    # time.sleep(60/(request_per_minute + 1))






 
# save data to hadoop hdfs 