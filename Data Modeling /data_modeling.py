from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CreateTables") \
    .config("hive.metastore.uris", "thrift://localhost:9084")\
    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/Binance_Data/warehouse/")\
    .enableHiveSupport() \
    .getOrCreate()

# Define SQL queries to create tables with relationships
create_tables_sql = """
-- Create Symbol_Infor table
CREATE TABLE IF NOT EXISTS Symbol_Infor (
  symbol_id INT PRIMARY KEY,
  symbol_name STRING,
  status STRING,
  baseAsset STRING,
  baseAssetPrecision INT,
  quoteAsset STRING,
  quotePrecision INT,
  quoteAssetPrecision INT,
  baseCommissionPrecision INT,
  quoteCommissionPrecision INT,
  filtertype_id INT,
  icebergAllowed BOOLEAN,
  ocoAllowed BOOLEAN,
  quoteOrderQtyMarketAllowed BOOLEAN,
  allowTrailingStop BOOLEAN,
  cancelReplaceAllowed BOOLEAN,
  isSpotTradingAllowed BOOLEAN,
  isMarginTradingAllowed BOOLEAN,
  defaultSelfTradePreventionMode STRING
);

-- Create Trades table with a foreign key relationship to Symbol_Infor
CREATE TABLE IF NOT EXISTS Trades (
  Trade_id INT,
  symbol_id INT REFERENCES Symbol_Infor(symbol_id),
  price DECIMAL(20, 10),
  qty DECIMAL(20, 10),
  time TIMESTAMP,
  isBuyerMaker BOOLEAN,
  isBestMatch BOOLEAN,
  PRIMARY KEY (Trade_id, symbol_id)
);

-- Create Filters table
CREATE TABLE IF NOT EXISTS Filters (
  filter_id INT PRIMARY KEY,
  price_filter_id INT REFERENCES Price_filter_detail(id),
  lot_size_filter_id INT REFERENCES Lot_size_filter_detail(id),
  iceberg_parts_filter_id INT REFERENCES Iceberg_parts_filter_detail(id),
  market_lot_size_filter_id INT REFERENCES Market_lot_size_filter_detail(id),
  trailing_delta_filter_id INT REFERENCES Trailing_delta_filter_detail(id),
  percent_price_by_side_filter_id INT REFERENCES Percent_price_by_side_filter_detail(id),
  notional_filter_id INT REFERENCES Notional_filter_detail(id),
  max_num_orders_filter_id INT REFERENCES Max_num_orders_filter_detail(id),
  max_num_algo_orders_filter_id INT REFERENCES Max_num_algo_orders_filter_detail(id)
);

-- Create Price_filter_detail table
CREATE TABLE IF NOT EXISTS Price_filter_detail (
  id INT PRIMARY KEY,
  maxPrice DECIMAL(20, 10),
  minPrice DECIMAL(20, 10),
  tickSize DECIMAL(20, 10)
);

-- Create Lot_size_filter_detail table
CREATE TABLE IF NOT EXISTS Lot_size_filter_detail (
  id INT PRIMARY KEY,
  stepSize DECIMAL(20, 10),
  maxQty DECIMAL(20, 10),
  minQty DECIMAL(20, 10)
);

-- Create Iceberg_parts_filter_detail table
CREATE TABLE IF NOT EXISTS Iceberg_parts_filter_detail (
  id INT PRIMARY KEY,
  limit INT
);

-- Create Market_lot_size_filter_detail table
CREATE TABLE IF NOT EXISTS Market_lot_size_filter_detail (
  id INT PRIMARY KEY,
  stepSize DECIMAL(20, 10),
  maxQty DECIMAL(20, 10),
  minQty DECIMAL(20, 10)
);

-- Create Trailing_delta_filter_detail table
CREATE TABLE IF NOT EXISTS Trailing_delta_filter_detail (
  id INT PRIMARY KEY,
  minTrailingBelowDelta DECIMAL(20, 10),
  maxTrailingBelowDelta DECIMAL(20, 10),
  maxTrailingAboveDelta DECIMAL(20, 10),
  minTrailingAboveDelta DECIMAL(20, 10)
);

-- Create Percent_price_by_side_filter_detail table
CREATE TABLE IF NOT EXISTS Percent_price_by_side_filter_detail (
  id INT PRIMARY KEY,
  bidMultiplierUp DECIMAL(20, 10),
  askMultiplierUp DECIMAL(20, 10),
  bidMultiplierDown DECIMAL(20, 10),
  avgPriceMins DECIMAL(20, 10),
  askMultiplierDown DECIMAL(20, 10)
);

-- Create Notional_filter_detail table
CREATE TABLE IF NOT EXISTS Notional_filter_detail (
  id INT PRIMARY KEY,
  maxNotional DECIMAL(20, 10),
  minNotional DECIMAL(20, 10),
  avgPriceMins DECIMAL(20, 10),
  applyMinToMarket BOOLEAN,
  applyMaxToMarket BOOLEAN
);

-- Create Max_num_orders_filter_detail table
CREATE TABLE IF NOT EXISTS Max_num_orders_filter_detail (
  id INT PRIMARY KEY,
  maxNumOrders INT
);

-- Create Max_num_algo_orders_filter_detail table
CREATE TABLE IF NOT EXISTS Max_num_algo_orders_filter_detail (
  id INT PRIMARY KEY,
  maxNumAlgoOrders INT
);

-- Create Ticker_24h table with a foreign key relationship to Symbol_Infor
CREATE TABLE IF NOT EXISTS Ticker_24h (
  Ticker_id INT PRIMARY KEY,
  symbol_id INT REFERENCES Symbol_Infor(symbol_id),
  priceChange DECIMAL(20, 10),
  priceChangePercent DECIMAL(20, 10),
  weightedAvgPrice DECIMAL(20, 10),
  prevClosePrice DECIMAL(20, 10),
  lastPrice DECIMAL(20, 10),
  bidPrice DECIMAL(20, 10),
  askPrice DECIMAL(20, 10),
  openPrice DECIMAL(20, 10),
  highPrice DECIMAL(20, 10),
  lowPrice DECIMAL(20, 10),
  volume DECIMAL(20, 10),
  openTime TIMESTAMP,
  closeTime TIMESTAMP,
  fristTradeId INT,
  lastTradeId INT,
  TradeCount INT
);

-- Create Klines table with a foreign key relationship to Symbol_Infor
CREATE TABLE IF NOT EXISTS Klines (
  kline_id INT PRIMARY KEY,
  symbol_id INT REFERENCES Symbol_Infor(symbol_id),
  Opentime TIMESTAMP,
  OpenPrice DECIMAL(20, 10),
  HighPrice DECIMAL(20, 10),
  LowPrice DECIMAL(20, 10),
  ClosePrice DECIMAL(20, 10),
  Volume DECIMAL(20, 10),
  Closetime TIMESTAMP,
  Quoteassetvolume DECIMAL(20, 10),
  Numberoftrades INT,
  Takerbaseassetvolume DECIMAL(20, 10),
  Takerbuyquoteassetvolume DECIMAL(20, 10),
  ignored DECIMAL(20, 10)
)
"""

# Execute the table creation queries
spark.sql(create_tables_sql)

# Stop the Spark session
spark.stop()