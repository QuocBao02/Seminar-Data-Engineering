from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

# Tạo phiên Spark
spark = SparkSession.builder.appName("TransformData").getOrCreate()

# Dữ liệu ban đầu
data = [
    {
        "value": [
            {
                "symbol": "KSMBTC",
                "trade": [
                    {
                        "id": 204,
                        "price": "0.00062700",
                        "qty": "3.55800000",
                        "quoteQty": "0.00223086",
                        "time": 1697076298465,
                        "isBuyerMaker": False,
                        "isBestMatch": True
                    },
                    # Thêm các giao dịch khác ở đây
                ]
            },
            {
                "symbol": "KSMUSDT",
                "trade": [
                    {
                        "id": 2047,
                        "price": "16.96000000",
                        "qty": "17.43900000",
                        "quoteQty": "295.76544000",
                        "time": 1697076305053,
                        "isBuyerMaker": True,
                        "isBestMatch": True
                    },
                    # Thêm các giao dịch khác ở đây
                   ]
            }
        ]
        
    }
]

# Tạo DataFrame
df = spark.createDataFrame(data, ["value"])

# Sử dụng PySpark để biến đổi DataFrame
result_df = df.select(explode(col("value").getItem("trade")).alias("trade"))

# Hiển thị kết quả
result_df.show(truncate=False)
