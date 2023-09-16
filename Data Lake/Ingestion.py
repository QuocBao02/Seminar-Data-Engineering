# import library 
import binance


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
# change type of data 

# create directory to save data into data lake in hadoop hdfs
 
 
# save  