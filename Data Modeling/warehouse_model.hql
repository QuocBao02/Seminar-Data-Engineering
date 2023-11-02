-- check and create Binance Market Data in warehouse if it dose not exist.
CREATE DATABASE IF NOT EXISTS Binance_Market_Data;

-- check and create Symbol Information table 
CREATE TABLE IF NOT EXISTS Symbol_Infor (
  symbol_id INT PRIMARY KEY,
  symbol_name STRING,
  status STRING,
  baseAsset STRING,
  baseAssetPrecision INT,
  quoteAsset STRING,
  quotePrecision INT,
  icebergAllowed BOOLEAN
);
-- check and create Ticker 24h table 
CREATE TABLE IF NOT EXISTS Ticker_24h (
  ticker_id INT,
  symbol_id STRING,
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
  tradeCount INT,
  PRIMARY KEY (ticker_id),
  FOREIGN KEY (symbol_id) REFERENCES Symbol_Infor(symbol_id)
);

-- check and create  Trades table 
CREATE TABLE IF NOT EXISTS Trades (
  trade_id INT,
  symbol_id STRING,
  price DECIMAL(20, 10),
  qty DECIMAL(20, 10),
  time TIMESTAMP,
  isBuyerMaker BOOLEAN,
  isBestMatch BOOLEAN,
  PRIMARY KEY (trade_id),
  FOREIGN KEY (symbol_id) REFERENCES Symbol_Infor(symbol_id)
);

-- check and create Klines table 
CREATE TABLE IF NOT EXISTS Klines (
  kline_id INT,
  symbol_id STRING,
  opentime TIMESTAMP,
  openPrice DECIMAL(20, 10),
  highPrice DECIMAL(20, 10),
  lowPrice DECIMAL(20, 10),
  closePrice DECIMAL(20, 10),
  volume DECIMAL(20, 10),
  closetime TIMESTAMP,
  quoteassetvolume DECIMAL(20, 10),
  numberoftrades INT,
  takerbaseassetvolume DECIMAL(20, 10),
  takerbuyquoteassetvolume DECIMAL(20, 10),
  ignored DECIMAL(20, 10),
  PRIMARY KEY (kline_id),
  FOREIGN KEY (symbol_id) REFERENCES Symbol_Infor(symbol_id)
);