from collections import defaultdict, deque
from shioaji import BidAskFOPv1, Exchange
import shioaji as sj
import datetime
import pandas as pd
import talib as ta
import time
from math import ceil
import pytrader as pyt

# api key
api_key=''
secret_key=''

# initial api
api = sj.Shioaji(simulation=True)
accounts = api.login(
    api_key=api_key,
    secret_key=secret_key
)

# initial order
trader = pyt.pytrader(strategy='108062213', api_key=api_key, secret_key=secret_key) 
# set contract
trader.contract('TXF')
contract = min(
    [
        x for x in api.Contracts.Futures.TXF
        if x.code[-2:] not in ["R1", "R2"]
    ],
    key=lambda x: x.delivery_date
)

msg_queue = defaultdict(deque)
api.set_context(msg_queue)

@api.on_bidask_fop_v1(bind=True)
def quote_callback(self, exchange: Exchange, bidask: BidAskFOPv1):
    # append quote to message queue
    self['bidask'].append(bidask)

# contract subscribe
api.quote.subscribe(
    contract,
    quote_type=sj.constant.QuoteType.BidAsk,
    version=sj.constant.QuoteVersion.v1
)

time.sleep(2.5)

# calculate Kbar
# get maximum strategy kbars to dataframe, extra 30 it's for safety
bars = 65 + 30
# since every day has 60 kbars (only from 8:45 to 13:45), for 5 minuts kbars
days = ceil(bars/60)

df_5min = []
while(len(df_5min) < bars):
    kbars = api.kbars(
        contract=api.Contracts.Futures.TXF.TXFR1,
        start=(datetime.date.today() -
               datetime.timedelta(days=days)).strftime("%Y-%m-%d"),
        end=datetime.date.today().strftime("%Y-%m-%d"),
    )
    df = pd.DataFrame({**kbars})
    df.ts = pd.to_datetime(df.ts)
    df = df.set_index('ts')
    df.index.name = None
    df = df[['Open', 'High', 'Low', 'Close', 'Volume']]
    df = df.between_time('00:00:00', '23:59:59')
    df_5min = df.resample('5T', label='right', closed='right').agg(
        {'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum'
         })
    df_5min.dropna(axis=0, inplace=True)
    days += 1

ts = datetime.datetime.now()


while datetime.datetime.now().time() < datetime.time(13, 40):
    time.sleep(2)
    # sl and tp
    if(len(trader.position()) == 0):
        self_position = 'None'
    else:
        self_position = 'Buy' if trader.position()['is_long'] else 'Sell'
    
    if self_position == 'Buy':
        if trader.position()['pl_pct'] <= -0.005 or trader.position()['pl_pct'] >= 0.03:
            trader.sell(size = 1)
    elif self_position == 'Sell':
        if trader.position()['pl_pct'] <= -0.005 or trader.position()['pl_pct'] >= 0.03:
            trader.buy(size = 1)

    # local time > next kbars time
    if(datetime.datetime.now() >= ts):
        kbars = api.kbars(
            contract=api.Contracts.Futures.TXF.TXFR1,
            start=datetime.date.today().strftime("%Y-%m-%d"),
            end=datetime.date.today().strftime("%Y-%m-%d"),
        )
        df = pd.DataFrame({**kbars})
        df.ts = pd.to_datetime(df.ts)
        df = df.set_index('ts')
        df.index.name = None
        df = df[['Open', 'High', 'Low', 'Close', 'Volume']]
        df = df.between_time('00:00:00', '23:59:59')
        df = df.resample('5T', label='right', closed='right').agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum'})
        df.dropna(axis=0, inplace=True)
        df_5min.update(df)
        to_be_added = df.loc[df.index.difference(df_5min.index)]
        df_5min = pd.concat([df_5min, to_be_added])
        ts = df_5min.iloc[-1].name.to_pydatetime()

        # next kbar time update and local time < next kbar time
        if (datetime.datetime.now().minute != ts.minute):

            df_5min = df_5min[:-1]

            self_adx28 = ta.ADX(df_5min['High'], df_5min['Low'], df_5min['Close'], 28)
            self_rsi13 = ta.RSI(df_5min['Close'], 13)
            self_adx15 = ta.ADX(df_5min['High'], df_5min['Low'], df_5min['Close'], 15)
            self_adx20 = ta.ADX(df_5min['High'], df_5min['Low'], df_5min['Close'], 20)
            self_rsi5 = ta.RSI(df_5min['Close'], 5)
            self_slope1 = ta.LINEARREG_SLOPE(df_5min['Close'], 12) 
            self_slope2 = ta.LINEARREG_SLOPE(self_rsi5, 12)
            
            if(len(trader.position()) == 0):
                self_position = 'None'
            else:
                self_position = 'Buy' if trader.position()['is_long'] else 'Sell'

            condition1 = datetime.datetime.now().time() < datetime.time(13, 15) and datetime.datetime.now().time() > datetime.time(9, 15) # operation time
            condition2 = trader.trades and (self_position == 'None') and (trader.trades[-1]['pnl']<0) and datetime.datetime.now()<(trader.trades[-1]['exit_time'].to_pydatetime()+datetime.timedelta(minutes=15)) # wait for 2 bars
            condition3 = datetime.datetime.now().time() >= datetime.time(13, 25) # close position time
            condition4 = (self_rsi13[-1] >= 70) and (self_adx28[-1] <= 15) and self_position == 'Buy'
            condition5 = (self_rsi13[-1] <= 20) and (self_adx28[-1] <= 15) and self_position == 'Sell'
            condition6 = ((70<=self_rsi13[-2]) and (70>=self_rsi13[-1])) and (self_adx28[-1]>=15) and self_position == 'None'
            condition7 = ((self_rsi13[-2]<=20) and (self_rsi13[-1]>=20)) and (self_adx28[-1]>=15) and self_position == 'None'
            condition8 = (self_rsi5[-1]>40) and self_adx20[-1]>=15 and (self_slope1[-3:]>=0).all() and (self_slope2[-4:]>=0).all() and (self_position == 'None' or self_position == 'Sell')
            condition9 = (self_rsi5[-1]<45) and self_adx20[-1]>=15 and (self_slope1[-3:]<=0).all() and (self_slope2[-4:]<=0).all() and (self_position == 'None' or self_position == 'Buy')
            condition10 = (self_rsi5[-1]>=45) and self_adx20[-1]>=15 and (self_slope1[-3:]<=0).all() and (self_slope2[-4:]>=0).all() and self_position == 'None'
            condition11 = (self_rsi5[-1]<=40) and self_adx20[-1]>=15 and (self_slope1[-3:]>=0).all() and (self_slope2[-4:]<=0).all() and self_position == 'None'
            condition12 = (df_5min['Close'][-4:]>df_5min['Open'][-4:]).all() and self_position == 'Sell'
            condition13 = (df_5min['Close'][-3:]<df_5min['Open'][-3:]).all() and self_position == 'Buy'
            
            if condition1:
                if condition2:
                    pass
                elif condition4:
                    trader.sell(size = 1)
                elif condition5:
                    trader.buy(size = 1)
                elif condition6:
                    trader.sell(size = 1)
                elif condition7:
                    trader.buy(size = 1)
                elif condition8:
                    trader.buy(size = 1)
                elif condition9:
                    trader.sell(size = 1)
                elif condition10:
                    trader.buy(size = 1)
                elif condition11:
                    trader.sell(size = 1)
                elif condition12:
                    trader.buy(size = 1)
                elif condition13:
                    trader.sell(size = 1)
            elif condition3:
                if self_position == 'Buy':
                    trader.sell(size = 1)
                elif self_position == 'Sell':
                    trader.buy(size = 1)


api.quote.unsubscribe(
    contract,
    quote_type=sj.constant.QuoteType.BidAsk,
    version=sj.constant.QuoteVersion.v1
)

api.logout()
