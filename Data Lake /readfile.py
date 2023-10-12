from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DecimalType
import json 
# Khởi tạo phiên làm việc Spark
# spark = SparkSession.builder.appName("Read from HDFS").getOrCreate()
spark = SparkSession.builder \
            .appName("Save to DataLake") \
            .config("spark.driver.memory", "6g") \
            .config("spark.executor.memory", "3g") \
            .config("spark.executor.cores", "4") \
            .getOrCreate()
# Đường dẫn trên HDFS đến tệp bạn muốn đọc
hdfs_path = 'hdfs://localhost:9000/user/Binance_Data/lake/klines/year=2023/month=10/day=12/klines.json'

# Đọc tệp từ HDFS thành DataFrame
# df = spark.read.json(hdfs_path)
# Hiển thị nội dung của DataFrame
# df.select("value").show(truncate=False)



# local_output_path = '/home/file.txt'
# df.select("value").write.mode("overwrite").text("file.txt")
# df.write.mode("overwrite").json("file.json")
# Dừng phiên
#  làm việc Spark





# # Định nghĩa schema cho dữ liệu JSON
# custom_schema = StructType([
#     StructField("value", ArrayType(
#         StructType([
#             StructField("symbol", StringType(), nullable=True),
#             StructField("trade", ArrayType(
#                 StructType([
#                     StructField("id", IntegerType(), nullable=True),
#                     StructField("price", DecimalType(10, 8), nullable=True),
#                     StructField("qty", DecimalType(10, 8), nullable=True),
#                     StructField("quoteQty", DecimalType(10, 8), nullable=True)
#                 ])
#             ))
#         ])
#     ))
# ])

# custom_schema = StructType([
#     StructField("value", StringType(), nullable=True),
# ])


df = spark.read.text(hdfs_path)

# Hiển thị nội dung của DataFrame
json_data = df.select("value").first()[0]
# print(json_data)
# df.show()
# df.write.mode("overwrite").json("file.json")

spark.stop()

