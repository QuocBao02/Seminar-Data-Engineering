def Trades():
    return "CREATE TABLE Trades(Trade_id BIGINT PRIMARY KEY, \
symbol_id INT PRIMARY KEY, \
isBuyerMaker BOOLEAN, \
isBestMatch BOOLEAN, \
price DECIMAL(20, 10), \
qty DECIMAL(20, 10), \
quoteQty DECIMAL(20, 10), \
time TIMESTAMP);"

def Symbol_Infor():
    return "CREATE TABLE Symbol_Infor(symbol_id INT PRIMARY KEY, \
symbol_name STRING, \
status STRING, \
baseAsset STRING, \
baseAssetPrecision INT, \
quoteAsset STRING, \
quotePrecision INT, \
quoteAssetPrecision INT, \
baseCommissionPrecision INT, \
quoteCommissionPrecision INT, \
filtertype_id INT REFERENCES Filters(filter_id), \
icebergAllowed BOOLEAN, \
ocoAllowed BOOLEAN, \
quoteOrderQtyMarketAllowed BOOLEAN, \
allowTrailingStop BOOLEAN, \
cancelReplaceAllowed BOOLEAN, \
isSpotTradingAllowed BOOLEAN, \
isMarginTradingAllowed BOOLEAN, \
defaultSelfTradePreventionMode STRING);"

def Filters():
    return "CREATE TABLE Filters(filter_id INT PRIMARY KEY);"

def Price_filter_detail():
    return "CREATE TABLE Price_filter_detail(id INT PRIMARY KEY, \
maxPrice DECIMAL(20, 10), \
minPrice DECIMAL(20, 10), \
tickSize DECIMAL(20, 10));"

def Lot_size_filter_detail():
    return "CREATE TABLE Lot_size_filter_detail(id INT PRIMARY KEY, \
stepSize DECIMAL(20, 10), \
maxQty DECIMAL(20, 10), \
minQty DECIMAL(20, 10));"

def Iceberg_parts_filter_detail():
    return "CREATE TABLE Iceberg_parts_filter_detail(id INT PRIMARY KEY, \
limit INT);"

def Market_lot_size_filter_detail():
    return "CREATE TABLE Market_lot_size_filter_detail(id INT PRIMARY KEY, \
stepSize DECIMAL(20, 10), \
maxQty DECIMAL(20, 10), \
minQty DECIMAL(20, 10));"

def Trailing_delta_filter_detail():
    return "CREATE TABLE Trailing_delta_filter_detail(id INT PRIMARY KEY, \
minTrailingBelowDelta DECIMAL(20, 10), \
maxTrailingBelowDelta DECIMAL(20, 10), \
maxTrailingAboveDelta DECIMAL(20, 10), \
minTrailingAboveDelta DECIMAL(20, 10));"

def Percent_price_by_side_filter_detail():
    return "CREATE TABLE Percent_price_by_side_filter_detail(id INT PRIMARY KEY, \
bidMultiplierUp DECIMAL(20, 10), \
askMultiplierUp DECIMAL(20, 10), \
bidMultiplierDown DECIMAL(20, 10), \
avgPriceMins DECIMAL(20, 10), \
askMultiplierDown DECIMAL(20, 10));"

def Notional_filter_detail():
    return "CREATE TABLE Notional_filter_detail(id INT PRIMARY KEY, \
maxNotional DECIMAL(20, 10), \
minNotional DECIMAL(20, 10), \
avgPriceMins DECIMAL(20, 10), \
applyMinToMarket BOOLEAN, \
applyMaxToMarket BOOLEAN);"

def Max_num_orders_filter_detail():
    return "CREATE TABLE Max_num_orders_filter_detail(id INT, \
maxNumOrders INT);"

def Max_num_algo_orders_filter_detail():
    return "CREATE TABLE Max_num_algo_orders_filter_detail(id INT, \
maxNumAlgoOrders INT);"

def Ticker_24h():
    return "CREATE TABLE Ticker_24h(Ticker_id BIGINT PRIMARY KEY, \
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
count BIGINT);"

def Klines():
    return "CREATE TABLE Klines(kline_id INT PRIMARY KEY, \
symbol_id INT REFERENCES Symbol_Infor(symbol_id), \
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
ignored DECIMAL(20, 10));"
