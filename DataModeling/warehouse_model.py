def Trades():
    return 
"CREATE TABLE IF NOT EXISTS Trades(trade_id BIGINT, \
symbol_id INT PRIMARY KEY, \
isBuyerMaker BOOLEAN, \
isBestMatch BOOLEAN, \
price DECIMAL(20, 10), \
qty DECIMAL(20, 10), \
quoteQty DECIMAL(20, 10), \
time TIMESTAMP \
PRIMARY KEY(trade_id, symbol_id),\
FOREIGN KEY (symbol_id) REFERENCES Symbol_Infor(symbol_id));"

def Symbol_Infor():
    return 
"CREATE TABLE IF NOT EXISTS Symbol_Infor(symbol_id INT, \
symbol_name STRING, \
status STRING, \
baseAsset STRING, \
baseAssetPrecision INT, \
quoteAsset STRING, \
quotePrecision INT, \
quoteAssetPrecision INT, \
baseCommissionPrecision INT, \
quoteCommissionPrecision INT, \
filtertype_id INT , \
icebergAllowed BOOLEAN, \
ocoAllowed BOOLEAN, \
quoteOrderQtyMarketAllowed BOOLEAN, \
allowTrailingStop BOOLEAN, \
cancelReplaceAllowed BOOLEAN, \
isSpotTradingAllowed BOOLEAN, \
isMarginTradingAllowed BOOLEAN, \
defaultSelfTradePreventionMode STRING, \
PRIMARY KEY (symbol_id), \
FOREIGN KEY (filter_id) REFERENCES Filters(filter_id));"

def Filters():
    return 
"CREATE TABLE IF NOT EXISTS Filters(filter_id INT, \
price_id INT, \
lot_size_filter_id INT, \
iceberg_parts_filter_id INT, \
market_lot_size_filter_id INT, \
trailing_delta_filter_id INT, \
percent_price_by_side_filter_id INT, \
notional_filter_id INT, \
max_num_orders_filter_id INT, \
max_num_algo_orders_filter_id INT, \
PRIMARY KEY (filter_id), \
FOREIGN KEY (price_filter_id) REFERENCES Price_filter_detail(id), \
FOREIGN KEY (lot_size_filter_id) REFERENCES Lot_size_filter_detail(id), \
FOREIGN KEY (iceberg_parts_filter_id) REFERENCES Iceberg_parts_filter_detail(id), \
FOREIGN KEY (market_lot_size_filter_id) REFERENCES Market_lot_size_filter_detail(id), \
FOREIGN KEY (trailing_delta_filter_id) REFERENCES Trailing_delta_filter_detail(id), \
FOREIGN KEY (percent_price_filter_id) REFERENCES Percent_price_filter_detail(id), \
FOREIGN KEY (notional_filter_id) REFERENCES Notional_filter_detail(id), \
FOREIGN KEY (max_num_orders_filter_id) REFERENCES Max_num_orders_filter_detail(id), \
FOREIGN KEY (max_num_algo_orders_filter_id) REFERENCES Max_num_algo_orders_filter_detail(id), \);"

def Price_filter_detail():
    return 
"CREATE  Price_filter_detail(id INT, \
maxPrice DECIMAL(20, 10), \
minPrice DECIMAL(20, 10), \
tickSize DECIMAL(20, 10), \
PRIMARY KEY (id));"

def Lot_size_filter_detail():
    return 
"CREATE TABLE IF NOT EXISTS Lot_size_filter_detail(id INT, \
stepSize DECIMAL(20, 10), \
maxQty DECIMAL(20, 10), \
minQty DECIMAL(20, 10), \
PRIMARY KEY(id));"

def Iceberg_parts_filter_detail():
    return 
"CREATE TABLE IF NOT EXISTS Iceberg_parts_filter_detail(id INT, \
limit INT, \
PRIMARY KEY(id));"

def Market_lot_size_filter_detail():
    return 
"CREATE TABLE IF NOT EXISTS Market_lot_size_filter_detail(id INT, \
stepSize DECIMAL(20, 10), \
maxQty DECIMAL(20, 10), \
minQty DECIMAL(20, 10), \
PRIMARY KEY(id));"

def Trailing_delta_filter_detail():
    return 
"CREATE TABLE IF NOT EXISTS Trailing_delta_filter_detail(id INT, \
minTrailingBelowDelta DECIMAL(20, 10), \
maxTrailingBelowDelta DECIMAL(20, 10), \
maxTrailingAboveDelta DECIMAL(20, 10), \
minTrailingAboveDelta DECIMAL(20, 10), \
PRIMARY KEY(id));"

def Percent_price_by_side_filter_detail():
    return 
"CREATE TABLE IF NOT EXISTS Percent_price_by_side_filter_detail(id INT, \
bidMultiplierUp DECIMAL(20, 10), \
askMultiplierUp DECIMAL(20, 10), \
bidMultiplierDown DECIMAL(20, 10), \
avgPriceMins DECIMAL(20, 10), \
askMultiplierDown DECIMAL(20, 10), \
PRIMARY KEY(id));"

def Notional_filter_detail():
    return 
"CREATE TABLE IF NOT EXISTS Notional_filter_detail(id INT, \
maxNotional DECIMAL(20, 10), \
minNotional DECIMAL(20, 10), \
avgPriceMins DECIMAL(20, 10), \
applyMinToMarket BOOLEAN, \
applyMaxToMarket BOOLEAN, \
PRIMARY KEY(id));"

def Max_num_orders_filter_detail():
    return 
"CREATE TABLE IF NOT EXISTS Max_num_orders_filter_detail(id INT, \
maxNumOrders INT, \
PRIMARY KEY(id));"

def Max_num_algo_orders_filter_detail():
    return 
"CREATE TABLE IF NOT EXISTS Max_num_algo_orders_filter_detail(id INT, \
maxNumAlgoOrders INT, \
PRIMARY KEY(id));"

def Ticker_24h():
    return 
"CREATE TABLE IF NOT EXISTS Ticker_24h(ticker_id BIGINT, \
symbol_id INT REFERENCES Symbol_Infor(symbol_id), \
priceChange DECIMAL(20, 10), \
priceChangePercent DECIMAL(20, 10), \
weightedAvgPrice DECIMAL(20, 10), \
prevClosePrice DECIMAL(20, 10), \
lastPrice DECIMAL(20, 10), \
bidPrice DECIMAL(20, 10), \
askPrice DECIMAL(20, 10), \
openPrice DECIMAL(20, 10), \
highPrice DECIMAL(20, 10), \
lowPrice DECIMAL(20, 10), \
volume DECIMAL(20, 10), \
openTime TIMESTAMP, \
closeTime TIMESTAMP, \
fristTradeId BIGINT, \
lastTradeId BIGINT, \
count BIGINT, \
PRIMARY KEY(ticker_id, symbol_id), \
FOREIGN KEY(symbol_id) REFERENCES Symbol_Infor(symbol_id));"

def Klines():
    return 
"CREATE TABLE IF NOT EXISTS Klines(kline_id INT, \
symbol_id INT , \
opentime TIMESTAMP, \
openPrice DECIMAL(20, 10), \
highPrice DECIMAL(20, 10), \
lowPrice DECIMAL(20, 10), \
closePrice DECIMAL(20, 10), \
volume DECIMAL(20, 10), \
closetime TIMESTAMP, \
quoteassetvolume DECIMAL(20, 10), \
numberoftrades INT, \
takerbaseassetvolume DECIMAL(20, 10), \
takerbuyquoteassetvolume DECIMAL(20, 10), \
ignored DECIMAL(20, 10), \
PRIMARY KEY (kline_id, symbol_id), \
FOREIGN KEY (symbol_id) REFERENCES Symbol_Infor(symbol_id));"
