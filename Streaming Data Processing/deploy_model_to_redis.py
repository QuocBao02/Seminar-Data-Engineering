from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col, from_unixtime, year, month, dayofmonth, hour, minute, second, sum, avg, round, from_json, current_timestamp
from pyspark.sql.types import StructType, StringType, StructField, LongType, BooleanType, TimestampType
import redis 


def feature_engineer(streaming_df):
    transformed_df = streaming_df\
        .select("T", "q", "p", "timestamp")\
        .withColumn("price", col("p").cast("float"))\
        .withColumn("qty", col("q").cast("float"))\
        .drop("p", "q")\
        .withColumn('trade_time', from_unixtime(col('T')/1000))\
        .withColumn('year', year(col('trade_time')))\
        .withColumn('month', month(col('trade_time')))\
        .withColumn('day', dayofmonth(col('trade_time')))\
        .withColumn('hour', hour(col('trade_time')))\
        .withColumn('minute', minute(col('trade_time')))\
        .withColumn('second', second(col('trade_time')))\
        .drop("T")\
        .drop("trade_time")\
        .na.drop()
        # .agg(round(sum("qty"), 6).alias("qty"), round(avg("price"), 6).alias("price"))    
            
            
        # .groupBy("year", "month", "day", "hour", "minute", "second")\
        # .agg(round(sum("qty"), 6).alias("qty"), round(avg("price"), 6).alias("price"))\
        # .orderBy("year", "month", "day", "hour", "minute", "second")
    return transformed_df


# def send_streaming_df_to_redis(df, r_host = "localhost", r_port = 6379, redis_key = "data_prediction"):
#         def send_to_redis(rdd):
#             r = redis.StrictRedis(host = r_host, port=r_port)
#             for record in rdd.collect():
#                 print(str(record))
#                 r.lpush(redis_key, str(record))
#         df.writeStream.foreachBatch(send_to_redis)
#         print("Finished")


# def process_streaming_data(df):
#       # Collect the DataFrame as a list of dictionaries (rows)
#     rows = df.collectAsMap()

#     # Convert rows to a dictionary of lists (group by key)
#     data_dict = {}
#     for row in rows:
#         key = row["key_column"]  # Replace with your actual key column
#         data_dict.setdefault(key, []).append(row)

#     # Process the dictionary of lists further (optional)
#     # ... your processing logic ...

#     return data_dict

# def writeToRedis(row):
#     redis_port = 6379
#     r = redis.Redis(host ="localhost", port=redis_port, decode_responses=True)
#     # Extract necessary fields from the row
#     year = row["year"]
#     month = row["month"]
#     day = row["day"]
#     hour = row["hour"]
#     minute = row["minute"]
#     second = row["second"]
#     qty = row["qty"]
#     price = row["price"]
#     prediction = row["prediction"]
#     # Construct a key for Redis (Example key: "2024-04-12-07-16-50")
#     key = f"{year}-{month:02d}-{day:02d}-{hour:02d}-{minute:02d}-{second:02d}"
    
#     # Construct a value for Redis (Example value: "0.00569,70322.554688,7.7269603395947115")
#     value = f"{qty},{price},{prediction}"
#     r.set(key, value)
    
    
def row_to_dict(row):
    return row.asDict()

def add_to_redis(row):
    r = redis.Redis(host="localhost", port=6379, db=0)
    r.xadd("prediction_data_streaming", row)
    print("Add to redis successfuly!")
   
   
def prediction_row_by_row(row, model):
    try:
        prediction=model.transform(row.toDF())
    except:
        prediction = None 
        print("error")
    return prediction
     
    
def main():
    # Create a SparkSession with necessary Kafka package
    spark = SparkSession.builder \
        .appName("StreamingExample") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .config("spark.redis.host", "localhost")\
        .config("spark.redis.port", "6379")\
        .getOrCreate()
    print("Create Spark Session Successfully!")    
    
    # load model 
    hdfs_path = "hdfs://localhost:9000/binance_streaming_data/predict_model/"
    model = PipelineModel.load(hdfs_path)
    print("Load Model Successfully!")
    
    
    schema = StructType([
        StructField("e_", StringType(), True),
        StructField("E", LongType(), True),
        StructField("s", StringType(), True),
        StructField("t_", LongType(), True),
        StructField("p", StringType(), True),
        StructField("q", StringType(), True),
        StructField("b", LongType(), True),
        StructField("a", LongType(), True),
        StructField("T", LongType(), True),
        StructField("m_", BooleanType(), True),
        StructField("M", BooleanType(), True)
    ])
    
    # Read streaming data from a source (e.g., Kafka)
    streaming_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("startingOffsets", "latest")\
        .option("subscribe", "test_init") \
        .load()\
        .selectExpr("CAST(value AS STRING) as value", "timestamp") \
        .select(from_json("value", schema).alias("data"), col("timestamp")) \
        .select("data.*", "timestamp")\

    # Add watermark to handle late data
    streaming_df_with_watermark = streaming_df.withWatermark("timestamp", "10 minutes")
    
    # Transform each row into columns of a DataFrame
    transformed_df = feature_engineer(streaming_df_with_watermark)
    
    transformed_df_without_watermark = feature_engineer(streaming_df)
    
    # Assemble the features into a feature vector
    assembler = VectorAssembler(
        inputCols=["year", "month", "day", "hour", "minute", "second", "qty", "price"],
        outputCol="streaming_features"
    )
    
    feature_df = assembler.transform(transformed_df)
    # apply prediction price 
    prediction = model.transform(feature_df).select("year", "month", "day", "hour", "minute", "second", "qty", "price", "prediction")


    # prediction = prediction.rdd.map(row_to_dict)
    
    # prediction.foreach(add_to_redis)
    # send_streaming_df_to_redis(prediction)
    # prediction.writeStream.format("redis")\
    #     .foreachBatch
    
    # json_df = prediction.toJSON()
    
    # for row in json_df.collect():
    #     print(row)
    
    
    streaming_query = prediction.writeStream \
    .format("console") \
    .option("queryName", "my_streaming_query") \
    .trigger(processingTime="10 seconds") \
    .outputMode("append")\
    .start()

    # # # Wait for termination (optional)
    # streaming_query.awaitTermination() 
    
    
    

    
    query = prediction\
            .writeStream\
            .outputMode("append")\
            .foreach(lambda row: add_to_redis(row_to_dict(row)))\
            .start()
    # prediction.writeStream.format("org.apache.spark.sql.redis")\
    #         .option("table", "test").save().start()
            # .outputMode("complete")\
            # .format("console")\
            # .start()
    # query.show(10)
    # # Keep the application running while the streaming query is active
    query.awaitTermination()
    
    
if __name__=="__main__":
    # redis_port = 6379
    # r = redis.Redis(host ="localhost", port=redis_port, decode_responses=True)
    main()