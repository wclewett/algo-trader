import requests, json
import datetime
import time
from OpenSSL import SSL
from config import *
from datetime import date, timedelta
import pandas as pd
from pandas import DataFrame
import numpy as np
import math
from textmagic.rest import TextmagicRestClient
import csv

API_KEY = "PKUDP2PADZFHAZEIQ0BC"
SECRET_KEY = "63HkgGBqFz4WSxxkfNa94Yuks49UPft79f72C7q5"
TIINGO_KEY = "e5c4e56edc97c91baff58c0e6629872e629a797e"

print("keys loaded")

# File path for rebalance counter
file_path = "data/rebalance.csv"
eqw_shares_file_path = "data/eqw_port.csv"
days_traded = pd.read_csv(file_path)

# Current Stocks in the Dow Jones 30
STOCKS = ['aapl', 'axp', 'ba', 'cat', 'csco','cvx', 'dis', 'dow','gs',
'hd', 'ibm', 'intc', 'jnj', 'jpm', 'ko', 'mcd', 'mmm', 'mrk', 'msft',
'nke', 'pfe', 'pg', 'rtx', 'trv', 'unh','v', 'vz', 'wba', 'wmt', 'xom']

# Target Weight for a Stock in the Portfolio as a Percentage of Current Portfolio Value
## Critical that this number when multiplied by 15 not exceed 1
target_weight = 0.065

# Rebalance Period in terms of days
rebalance_period = 30

# Relevant Dates - Inception Date is the initialization date, Start and End are used for 
# data retrieval
INCEPTION_DATE = datetime.date(2020,6,17)
END_DATE = date.today()
if date.today().weekday() == 0:
    START_DATE = date.today() - timedelta(3)
else:
    START_DATE = date.today() - timedelta(1)

# API Headers - Keys referenced in config.py
ALPACA = {'APCA-API-KEY-ID': API_KEY,'APCA-API-SECRET-KEY': SECRET_KEY}
TIINGO = {'Content-Type': 'application/json'}

# Tiingo API Endpoints
TIINGO_PRICING_URL = "https://api.tiingo.com/tiingo/daily/"
TIINGO_FUNDAMENTAL_URL = "https://api.tiingo.com/tiingo/fundamentals/"
START_DATE_URL = "startDate={}&".format(START_DATE)
END_DATE_URL = "endDate={}&".format(END_DATE)
TIINGO_TOKEN_URL = "token={}".format(TIINGO_KEY)
DATE_URL = START_DATE_URL + END_DATE_URL + TIINGO_TOKEN_URL

# Alpaca API Endpoints
BASE_URL = "https://paper-api.alpaca.markets"
ACCOUNT_URL = "{}/v2/account".format(BASE_URL)
ORDERS_URL = "{}/v2/orders".format(BASE_URL)
POSITIONS_URL = "{}/v2/positions".format(BASE_URL)
CALENDAR_URL = "{}/v2/calendar".format(BASE_URL)
PORTFOLIO_HISTORY_URL = "{}/v2/account/portfolio/history".format(BASE_URL)
MARKET_CLOCK_URL = "{}/v2/clock".format(BASE_URL)

# Print Current Time
print(datetime.datetime.now().strftime('%A %H:%M:%S %p'))

# Get and post API requests for Alpaca
def getAccount():
    r = requests.get(ACCOUNT_URL, headers=ALPACA)
    return json.loads(r.content)

def createOrder(symbol, qty, side, type, time_in_force):
    data = {
        "symbol": symbol,
        "qty": qty,
        "side": side,
        "type": type,
        "time_in_force": time_in_force,
    }
    r = requests.post(ORDERS_URL, json=data, headers=ALPACA)
    return json.loads(r.content)

def getOrders():
    r = requests.get(ORDERS_URL, headers=ALPACA)
    return json.loads(r.content)

def getPositions():
    r = requests.get(POSITIONS_URL, headers=ALPACA)
    return json.loads(r.content)

def getMarketClock():
    r = requests.get(MARKET_CLOCK_URL, headers=ALPACA)
    return json.loads(r.content)

# Get API requests from Tiingo data source
def retrievePricing():
    print("Retrieving Pricing Data...")
    x = {}
    for stock in STOCKS:
        r = requests.get(TIINGO_PRICING_URL + "{}/prices?".format(stock) + TIINGO_TOKEN_URL, headers=TIINGO)
        print("{}".format(stock), " Pricing Retrieved")
        x[stock] = pd.json_normalize(json.loads(r.content)).drop(["adjClose", "adjHigh", "adjLow", "adjOpen", "adjVolume", "divCash", "splitFactor"], axis=1)
        x[stock] = x[stock].drop(["high", "low", "open", "volume"], axis=1)
        x[stock].date = pd.to_datetime(x[stock].date).dt.strftime('%Y-%m-%d')
    return x

def retrievePricingIEX():
    print("Retrieving Pricing Data...")
    x = {}
    for stock in STOCKS:
        r = requests.get(TIINGO_PRICING_URL + f"?tickers={stock}&" + TIINGO_TOKEN_URL, headers=TIINGO)
        print(f"{stock}", " Pricing Retrieved")
        x[stock] = pd.json_normalize(json.loads(r.content)).drop(["ticker", "quoteTimestamp", "lastSaleTimeStamp",
         "last", "lastSize", "prevClose","open", "high", "low", "mid", "volume",
         "bidSize", "bidPrice", "askSize", "askPrice"])
    return x

def retrieveFundamental():
    print("Retrieving Fundamental Data...")
    x = {}
    for stock in STOCKS:
        r = requests.get(TIINGO_FUNDAMENTAL_URL + "{}/daily?".format(stock) + DATE_URL, headers=TIINGO)
        print("{}".format(stock), " Fundamentals Retrieved")
        x[stock] = pd.json_normalize(json.loads(r.content)).drop(['marketCap', 'enterpriseVal', 'trailingPEG1Y', 'pbRatio'], axis=1)
        x[stock].date = pd.to_datetime(x[stock].date).dt.strftime('%Y-%m-%d')
    return x

# Data Wrangling Operations
def index_date(x):
    for stock in STOCKS:
        x[stock] = pd.DataFrame.set_index(x["{}".format(stock)], x["{}".format(stock)].date)
        x[stock] = x[stock].drop(["date"], axis=1)
    return x

def name_stock_cols(x):
    for stock in STOCKS:
        x[stock].columns = ["{}".format(stock)]
    return x

def merge_dictionary_df(y):
    x = y[STOCKS[0]]
    for i in range(0,29):
        x = pd.merge(x, y[STOCKS[i+1]], left_index=True, right_index=True)
    return x

def rank_df(x):
    x = x.rank(axis=1)
    return x

# Portfolio Structure Dataframes
def port_construct(x):
    print("Constructing Portfolio...")
    y = x
    for j in range(0,30):
        if x.iloc[0,j] > 15:
            y.iloc[0,j] = 1
        else:
            y.iloc[0,j] = 0
    y.columns = STOCKS
    print("Equities selected: ", int(y.values.sum(axis=1)))
    return y

def eqw_shares(port_construct,prices,target_weight):
    print("Equal Weighting Shares...")
    x = pd.DataFrame(data=(port_construct.values*prices.values), columns=port_construct.columns, index=port_construct.index)
    account = getAccount()
    dollar_target = np.around(int(float(account["portfolio_value"]))*(target_weight),decimals=2)
    y = np.array(x.div(dollar_target, fill_value=0))
    y = 1/y
    y = np.where(y > 1000000, 0, y)
    y = np.where((0 < y) & (y < 1), 1, y)
    y = np.around(y,decimals=0)
    y = pd.DataFrame(data=y, columns=port_construct.columns, index=port_construct.index)
    print("Equity Stake ","=", np.sum(y.values*prices.values, axis=1))
    return y

# Rebalancing Operations

def implement_rebalance(eqw_shares_df):
    x = getPositions()
    print("Positions Retrieved")
    for stock in STOCKS:
        print(f"Cross Referencing {stock.upper()}")
        for i in range(0,15):
            if f"{stock.upper()}" == x[i]["symbol"]:
                y = int(eqw_shares_df[f"{stock}"].values) - int(float(x[i]["qty"]))
                if y > 0:
                    symbol = str(stock).upper()
                    qty  = int(y)
                    side = str("buy")
                    type = str("market")
                    time_in_force = str("gtc")
                    createOrder(symbol=symbol,qty=qty,side=side,type=type,time_in_force=time_in_force)
                    print("Bought", y, str(stock).upper())
                if y < 0:
                    y = y * (-1)
                    symbol = str(stock).upper()
                    qty = int(y)
                    side = str("sell")
                    type = str("market")
                    time_in_force = str("gtc")
                    createOrder(symbol=symbol,qty=qty,side=side,type=type,time_in_force=time_in_force)
                    print("Sold", y, str(stock).upper())
                if y == 0:
                    print(f"{stock.upper()} shares in line with Equal Weight")
                y = 0
    print("Rebalance Orders Placed!")

def rebalance_check(eqw_shares_df):
    x = days_traded['count'][0]
    y = x/rebalance_period
    y = math.ceil(y)
    z = x/(rebalance_period*y)
    if z == 1:
        print("Initializing Rebalance...")
        implement_rebalance(eqw_shares_df)
        print("Rebalance Finished!")
    else:
        print("No Rebalance Necessary")
        print((rebalance_period*y) - x, "Days until next rebalance")

# Initialize Portfolio
def initialize_port(eqw_shares_df):
    if INCEPTION_DATE == date.today():
        print("Success! Initializing orders...")
        for stock in STOCKS:
            if int(eqw_shares_df["{}".format(stock)]) > 0:
                symbol = str(stock).upper()
                qty = int(eqw_shares_df["{}".format(stock)])
                side = str("buy")
                type = str("market")
                time_in_force = str("gtc")
                createOrder(symbol=symbol,qty=qty,side=side,type=type,time_in_force=time_in_force)
                print("Bought", str(stock).upper())
    else:
        print("Running Rebalance Check...")
        rebalance_check(eqw_shares_df)

# Run Portfolio
def run_port():
    market_clock = getMarketClock()
    if datetime.datetime.today().weekday() < 5:
        if market_clock["is_open"]:
            days_traded['count'][0] += 1
            djiaPrices = retrievePricing()
            djiaPE = retrieveFundamental()
            djiaPrices = index_date(djiaPrices)
            djiaPrices = name_stock_cols(djiaPrices)
            djiaPrices = merge_dictionary_df(djiaPrices)
            djiaPE = index_date(djiaPE)
            djiaPE = name_stock_cols(djiaPE)
            djiaPE = merge_dictionary_df(djiaPE)
            djiaPE_rank = rank_df(djiaPE)
            port_construct_df = port_construct(djiaPE_rank)
            eqw_port = eqw_shares(port_construct_df,djiaPrices,target_weight)
            initialize_port(eqw_port)
            with open(file_path, 'w', newline='') as csvfile:
                csvwriter = csv.writer(csvfile, delimiter=',')
                csvwriter.writerow(["count"])
                csvwriter.writerow(days_traded.iloc[0])
            with open(eqw_shares_file_path, 'w', newline='') as csvfile2:
                csvwriter = csv.writer(csvfile2, delimiter=',')
                csvwriter.writerow(STOCKS)
                csvwriter.writerow(eqw_port.iloc[0])

run_port()