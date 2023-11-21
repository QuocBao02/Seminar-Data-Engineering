from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, dense_rank
from pyspark.sql.window import Window
from pyspark.sql.functions import col,current_date, date_format, rank
from pyspark.sql.types import DateType, TimestampType

class ReportBuilder:
    def __init__(self, host, metastore_port, hdfs_port, database):
        self.host = host
        self.metastore_port = metastore_port
        self.hdfs_port = hdfs_port
        self.database = database
        self.spark = self.init_spark_session()

    def init_spark_session(self):
        spark = SparkSession.builder \
            .appName("ReportBuilder") \
            .config("hive.metastore.uris", f"thrift://{self.host}:{self.metastore_port}") \
            .config("spark.sql.warehouse.dir", f"hdfs://{self.host}:{self.hdfs_port}/user/Binance_Data/warehouse/") \
            .enableHiveSupport() \
            .getOrCreate()
        return spark

    def create_daily_qty_top_symbols_table(self):

        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database};")
        self.spark.sql(f"USE {self.database};")
        # latestDF_value=self.spark.sql(f"SELECT {column_name} FROM trades ORDER BY {column_name} DESC LIMIT 1;")
        # Replace "trades_detail" with the actual table name for trades data
        trades_df = self.spark.table(f"{self.database}.trades")
        trades_df = trades_df.withColumn("timestamp_column", col("timestamp_column").cast(DateType()))
        trades_df = trades_df.filter(col("timestamp_column") == current_date())
        # Assuming trades_df has columns: 'symbol_id', 'qty'
        daily_qty_top_symbols = trades_df.groupBy('symbol_id').agg({'qty': 'sum'}).withColumnRenamed('sum(qty)', 'total_qty')

        # Rank symbols based on total_qty in descending order
        daily_qty_top_symbols = daily_qty_top_symbols.withColumn('rank', dense_rank().over(Window.orderBy(desc('total_qty'))))

        # Select top 10 symbols
        top_10_symbols = daily_qty_top_symbols.filter("rank <=5 ").select('symbol_id', 'total_qty')
        top_10_symbols.show()
        # Save the result as a new Hive table
        top_10_symbols.write.mode("overwrite").saveAsTable(f"{self.database}.daily_qty_top_symbols")


    def create_daily_qty_top_symbols_withTime_table(self):

        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database};")
        self.spark.sql(f"USE {self.database};")

        trades_df = self.spark.table(f"{self.database}.trades")
        daily_qty_top = self.spark.table(f"{self.database}.daily_qty_top_symbols")

        # Join the DataFrames
        df = daily_qty_top.join(trades_df, "symbol_id", "inner")

        # Filter based on a specific date
        df = df.filter(col("timestamp_column").cast(DateType()) == current_date())

        # Cast timestamp_column to TimestampType
        df = df.withColumn("timestamp_column", date_format("timestamp_column", "HH:mm:ss"))

        df = df.select("symbol_id", "timestamp_column", "qty")
        # Assuming trades_df has columns: 'symbol_id', 'qty'
        df.show()
        df = df.groupBy("symbol_id", "timestamp_column").agg({'qty': 'sum'}).withColumnRenamed('sum(qty)', 'total_qty').orderBy(desc("timestamp_column"))
        # Display the DataFrame
        df.write.mode("overwrite").saveAsTable(f"{self.database}.daily_qty_top_symbols_withTime")

    def create_daily_Price_BTCUSDT_withTime_table(self):

        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database};")
        self.spark.sql(f"USE {self.database};")

        df = self.spark.table(f"{self.database}.trades")

        # Filter based on a specific date
        df = df.filter(col("timestamp_column").cast(DateType()) == current_date())

        # Cast timestamp_column to TimestampType
        df = df.withColumn("timestamp_column", date_format("timestamp_column", "HH:mm:ss"))

        df = df.select("symbol_id", "timestamp_column", "price","qty").filter(col("symbol_id") == "BTCUSDT").dropDuplicates().orderBy(desc("timestamp_column"))
        # Assuming trades_df has columns: 'symbol_id', 'qty'
        df.show()
        # Display the DataFrame
        df.write.mode("overwrite").saveAsTable(f"{self.database}.daily_Price_BTCUSDT_withTime")


    def create_daily_quoteQty_top_symbols_table(self):

        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database};")
        self.spark.sql(f"USE {self.database};")
        # Replace "trades_detail" with the actual table name for trades data
        trades_df = self.spark.table(f"{self.database}.trades")
        trades_df = trades_df.withColumn("timestamp_column", col("timestamp_column").cast(DateType()))
        trades_df = trades_df.filter(col("timestamp_column") == current_date())

        # Assuming trades_df has columns: 'symbol_id', 'qty'
        daily_quoteQty_top_symbols = trades_df.groupBy('symbol_id').agg({'quoteQty': 'sum'}).withColumnRenamed('sum(quoteQty)', 'total_quoteQty')

        # Rank symbols based on total_qty in descending order
        daily_quoteQty_top_symbols = daily_quoteQty_top_symbols.withColumn('rank', dense_rank().over(Window.orderBy(desc('total_quoteQty'))))

        # Select top 10 symbols
        top_10_symbols = daily_quoteQty_top_symbols.filter("rank <= 5").select('symbol_id', 'total_quoteQty')
        top_10_symbols.show()
        # Save the result as a new Hive table
        top_10_symbols.write.mode("overwrite").saveAsTable(f"{self.database}.daily_quoteQty_top_symbols")
    

if __name__ == "__main__":
    # Replace these values with your actual configuration
    host = "localhost"
    metastore_port = 9084
    hdfs_port = 9000
    database = "Binance_Data"

    report_builder = ReportBuilder(host, metastore_port, hdfs_port, database)
    report_builder.create_daily_qty_top_symbols_table()
    report_builder.create_daily_quoteQty_top_symbols_table()
    report_builder.create_daily_qty_top_symbols_withTime_table()
    report_builder.create_daily_Price_BTCUSDT_withTime_table()
