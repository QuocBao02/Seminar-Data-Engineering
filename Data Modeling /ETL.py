from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import monotonically_increasing_id
import time
# spark = SparkSession.builder.appName("PySparkTest")\
#     .config("hive.exec.dynamic.partition.mode", "nonstrict")\
#     .config("hive.exec.dynamic.partition", "true")\
#     .config("spark.hadoop.hive.exec.dynamic.partition.mode", "non-strict")\
#     .config("spark.hadoop.hive.exec.dynamic.partition", "true")\
#     .config("hive.metastore.uris", "thrift://localhost:9084")\
#     .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/Binance_Data/warehouse/")\
#     .enableHiveSupport()\
#     .getOrCreate()
class PathGenerator:
    def __init__(self, hdfs_host="localhost", hdfs_port=9000, database = "Binance_Data"):
        self.hdfs_host = hdfs_host
        self.hdfs_port = hdfs_port
        self.database = database
        self.partition=self.get_partition_time()

    def get_partition_time(self):
        '''Create directory to save data into data lake in Hadoop HDFS'''
        # Get the current date and time
        now = datetime.now()
        partition = f'year={now.year}/month={now.month}/day={now.day}'
        return partition

    def get_generate_partition(self, table):
        '''Generate the path to save into data lake'''
        partition = self.get_partition_time()
        hdfs_path = f"hdfs://{self.hdfs_host}:{self.hdfs_port}/user/{self.database}/lake/{table}/{self.partition}/{table}.parquet"
        return hdfs_path
    
class ETLJob:
    def __init__(self, app_name, hive_metastore_uri, warehouse_dir):
        self.spark = SparkSession.builder.appName(app_name) \
            .config("hive.metastore.uris", hive_metastore_uri)\
            .config("spark.sql.warehouse.dir", warehouse_dir)\
            .config("hive.exec.dynamic.partition.mode", "nonstrict")\
            .config("hive.exec.dynamic.partition", "true")\
            .config("spark.hadoop.hive.exec.dynamic.partition.mode", "non-strict")\
            .config("spark.hadoop.hive.exec.dynamic.partition", "true")\
            .enableHiveSupport()\
            .getOrCreate()

    def extract_data(self, input_path):
        return self.spark.read.parquet(input_path, multiLine=True)

    def transform_data(self, input_df, transformation_logic):
        # Thực hiện các biến đổi dữ liệu tại đây bằng cách sử dụng transformation_logic cụ thể cho từng bảng
        transformed_df = transformation_logic(input_df)
        return transformed_df

    def load_data(self, db_name, table_name, output_df):
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name};")
        self.spark.sql(f"USE {db_name};")
        output_df.write.mode("overwrite").saveAsTable(table_name)

    def run(self, input_path, db_name, table_name, transformation_logic):
        input_data = self.extract_data(input_path)
        transformed_data = self.transform_data(input_data, transformation_logic)
        self.load_data(db_name, table_name, transformed_data)
        self.spark.stop()

# Định nghĩa các hàm biến đổi riêng cho từng bảng
def transform_table_trades(input_df):
    # Logic biến đổi cho bảng 1
    transformed_df = input_df.withColumn("trade_id", monotonically_increasing_id())
    columns = ["id", "symbol", "price", "qty", "quoteQty", "isBestMatch", "isBuyerMaker", "isBuyerMaker"]
    transformed_df = input_df.select( columns)
    transformed_df.show(20)
    transformed_df = input_df
    return transformed_df

def transform_table_ticker24h(input_df):
    # Logic biến đổi cho bảng 2
    transformed_df = input_df.filter(input_df["column3"] > 100)
    return transformed_df

if __name__ == "__main__":
    app_name = "ETL_lake2warehouse"
    metastore_uri = "thrift://localhost:9084"
    warehouse_dir = "hdfs://localhost:9000/user/Binance_Data/warehouse/"

    hdfs_host="localhost"
    hdfs_port=9000
    database_name = "Binance_Data"
    table_name = "trades"

    etl_job = ETLJob(app_name, metastore_uri, warehouse_dir)
    path = PathGenerator(hdfs_host="localhost", hdfs_port=9000,database = "Binance_Data")
    # ETL cho bảng 1
    input_path = path.get_generate_partition(table_name)
    table_name_table1 = "trades"
    etl_job.run(input_path, database_name, table_name, transform_table_trades)

    # # ETL cho bảng 2
    # input_path_table2 = "hdfs://localhost:9000/path/to/table2/data.csv"
    # database_name_table2 = "mydb"
    # table_name_table2 = "table2"
    # etl_job.run(input_path_table2, database_name_table2, table_name_table2, transform_table2)