import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import abs, col, when, from_unixtime, year, month, dayofmonth, hour, minute, second, sum, avg, round
from pyspark.ml.feature import VectorAssembler, PolynomialExpansion 
from pyspark.ml.regression import LinearRegression, LinearRegressionModel
from pyspark.ml import Pipeline, PipelineModel




def getPartitionTime(status = "current"):
    '''Create directory to save data into data lake in hadoop hdfs
    
    status: "current" or "previous"
    '''
    c_time=time.time()
    if status == "current":
        current_time=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    else:
        current_time=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(c_time - 86400))
    year=current_time.split()[0].split('-')[0]
    month=current_time.split()[0].split('-')[1]
    day=current_time.split()[0].split('-')[2]
    # month = '02'
    day = 21
    partition=f'year={year}/month={month}/day={day}'    
    return partition

class Polinomial_Model:
    def __init__(self, symbol, table, partition, feature_cols):
        self.symbol = symbol
        self.table = table 
        self.partition = partition 
        self.feature_cols = feature_cols
        self.spark = self._initSpark()
        self.raw_data = self._getDataFromDataLake()
        self.extracted_df = self._feature_engineering()
        
    def _initSpark(self):
        # connect to data lake 
        spark = SparkSession.builder.appName("ReadDataFromDataLake").getOrCreate()
        print("Connect to spark sucessfully!")
        return spark
    
    def _getDataFromDataLake(self):
        hdfs_path = f"hdfs://localhost:9000/Binance_Market_Data/DataLake/Trades/symbol={self.symbol}/{self.partition}/{self.table}.parquet"
        raw_data = self.spark.read.parquet(hdfs_path)
        return raw_data
    
    def _feature_engineering(self):
        extracted_df = self.raw_data.drop("id")\
                .drop("isBestMatch")\
                .drop("isBuyerMaker")\
                .drop("quoteQty")\
                .withColumn('timestamp', from_unixtime(col('time')/1000))\
                .drop("time")\
                .withColumn('year', year(col('timestamp')))\
                .withColumn('month', month(col('timestamp')))\
                .withColumn('day', dayofmonth(col('timestamp')))\
                .withColumn('hour', hour(col('timestamp')))\
                .withColumn('minute', minute(col('timestamp')))\
                .withColumn('second', second(col('timestamp')))\
                .drop("timestamp")\
                .withColumn("price", col("price").cast("float"))\
                .withColumn("qty", col("qty").cast("float"))\
                .groupBy("year", "month", "day", "hour", "minute", "second")\
                                .agg(round(sum("qty"), 6).alias("qty"), round(avg("price"), 6).alias("price"))\
                                .orderBy("year", "month", "day", "hour", "minute", "second")       
        return extracted_df
    
    def save_model(self, hdfs_path):
        self.model.write().overwrite().save(hdfs_path)
        self.model.write().overwrite().save("/home/quocbao/prediction/new_predict_model")
    
    def error(self, df):
        err_df = df.withColumn("error", abs(col("price") -col("prediction")))
        err_df.agg(sum("error")).show()
        
        
    def train_model(self, degree, train_rate, hdfs_path):
        
        # Split the data into training and testing sets
        (trainingData, testData) = self.extracted_df.randomSplit([train_rate, 1 - train_rate], seed=42)
        assembler = VectorAssembler(inputCols=self.feature_cols, outputCol="features")
        # Apply polynomial expansion to the features
        poly_expansion = PolynomialExpansion(degree=degree, inputCol="features", outputCol="expanded_features")

        
        # target_df.show(100)
        # training_df.show(100)
        # extracted_df.printSchema()
        # a = extracted_df.count()
        # print(a)
        
        
        # check and retrain model 
        # load old model
        try:
            historical_model = PipelineModel.load(hdfs_path)
            # print("Branch 1")
            retrain_model = historical_model.fit(trainingData)
        except:
            # init model 
            lr = LinearRegression(featuresCol="expanded_features", labelCol="price")
            pipeline = Pipeline(stages=[assembler, poly_expansion, lr])
            # prepare the features 
            retrain_model = pipeline.fit(trainingData)
            # print("Branch 2")
        
        # Make predictions on the test data
        predictions = retrain_model.transform(testData)
        # predictions = historical_model.transform(testData)

        
        # Show the predictions
        predictions.select("price", "prediction").show()
        self.error(predictions)
        
        self.model = retrain_model

        
    

def main():
    symbol = "SHIBUSDT"
    save_model_path = "hdfs://localhost:9000/binance_streaming_data/new_predict_model_test/"
    table = "Trades"
    feature_cols = ["year", "month", "day", "hour", "minute", "second", "qty"]
    partition = getPartitionTime(status="current")

    # degree = [3, 4, 5]
    # train_rate = [0.7, 0.8, 0.9] 
    # for d in degree:
    #     for r in train_rate:
    #         print(f"degree: {d}, rate: {r}")
    #         model.Model(d, r)
    
    # print(partition)
    
    train_rate = 0.9 
    degree = 3 

    model = Polinomial_Model(symbol, table, partition, feature_cols)
    model.train_model(degree, train_rate, save_model_path)
    model.save_model(save_model_path)   

if __name__=="__main__":
    main()


    



