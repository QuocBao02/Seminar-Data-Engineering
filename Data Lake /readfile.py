from pyspark.sql import SparkSession

# Khởi tạo phiên làm việc Spark
spark = SparkSession.builder.appName("Read from HDFS").getOrCreate()

# Đường dẫn trên HDFS đến tệp bạn muốn đọc
hdfs_path = 'hdfs://localhost:9000/user/Binance_Data/lake/symbol_infor/year=2023/month=10/day=12/symbol_infor.json'

# Đọc tệp từ HDFS thành DataFrame
df = spark.read.json(hdfs_path)
# Hiển thị nội dung của DataFrame
# df.select("value").show(truncate=False)



# local_output_path = '/home/file.txt'
df.select("value").write.mode("overwrite").text("file.txt")
# Dừng phiên làm việc Spark
spark.stop()