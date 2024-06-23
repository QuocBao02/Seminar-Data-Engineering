import redis, logging, sys
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.datastream.functions import ProcessFunction
from pyflink.common import SimpleStringSchema
from pyflink.common import Types
import json, traceback

# set up kafka topic, server 
kafka_topic="test_init"
kafka_bootstrap_server = "localhost:9092"
STREAM_KEY = "binance_data_stream"
# Set up Redis connection
redis_host = 'localhost'
redis_port = 6379

redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)


# trade data 
# {"e":"trade","E":1711163720724,"s":"BTCUSDT","t":3507725073,"p":"63735.37000000","q":"0.00039000","b":25945359006,"a":25945358272,"T":1711163720723,"m":false,"M":true}

def write_to_redis(record):
    try:
        print(record)
        redis_client = redis.Redis(host='localhost', port=6379)
        redis_client.xadd("my-stream", {"data": record})
        
    except Exception as e:
        # Log the error 
        print("Error writing to Redis:", e)
        traceback.print_exc()  # Print the stack trace

def main(env):
    # Define your Flink job here, for example reading from Kafka
    kafka_props={
        'bootstrap.servers': "localhost:9092",
    }
    # deserialization_schema = JsonRowDeserializationSchema.Builder() \
    # .type_info(Types.ROW([
    #     Types.STRING(),  # e
    #     Types.LONG(),    # E
    #     Types.STRING(),  # s
    #     Types.LONG(),    # t
    #     Types.STRING(),  # p
    #     Types.STRING(),  # q
    #     Types.LONG(),    # b
    #     Types.LONG(),    # a
    #     Types.LONG(),    # T
    #     Types.BOOLEAN(), # m
    #     Types.BOOLEAN()  # M
    # ])) \
    # .build()
    
    kafka_source=FlinkKafkaConsumer(topics="test_init", deserialization_schema = SimpleStringSchema(), properties=kafka_props)
    kafka_source.set_start_from_earliest()
    stream_data = env.add_source(kafka_source)
    stream_data.map(write_to_redis)
    # env.add_source(kafka_source).print()
    env.execute()

    # Add Kafka source to the Flink environment
    # env.add_source(kafka_source)
    # Execute the Flink job
    # env.execute()
    # spark_streaming_data.show()

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")    
    # Set up Flink environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:/home/quocbao/MyData/Build-Binance-s-Real-time-Data-Storage-Platform-with-the-Apache-Ecosystem-s-Big-Data-Tools./flink-sql-connector-kafka-1.15.0.jar")
    env.set_python_executable("/usr/bin/python3")
    main(env)