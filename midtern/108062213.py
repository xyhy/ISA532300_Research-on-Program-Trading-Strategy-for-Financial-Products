from collections import defaultdict, deque
from shioaji import BidAskFOPv1, Exchange, TickFOPv1
import shioaji as sj
import datetime
import pytz
import pandas as pd
import talib as ta
import time
# from time import time
from math import ceil
import pysimulation


api = sj.Shioaji(simulation=True)
accounts = api.login(
  api_key="",
  secret_key=""
)

order = pysimulation.order('108062213')

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

@api.on_tick_fop_v1(bind=True)
def quote_callback(self, exchange:Exchange, tick:TickFOPv1):
  self['tick'].append(tick)

api.quote.subscribe(
  contract,
  quote_type=sj.constant.QuoteType.BidAsk,
  version=sj.constant.QuoteVersion.v1
)

api.quote.subscribe(
    contract,
    quote_type = sj.constant.QuoteType.Tick,
    version = sj.constant.QuoteVersion.v1,
)

time.sleep(2.5)

# get maximum strategy kbars to dataframe, extra 30 it's for safety
bars = 65 + 30

# since every day has 60 kbars (only from 8:45 to 13:45), for 5 minuts kbars
days = ceil(bars/60)

df_5min = []
while(len(df_5min) < bars):
  kbars = api.kbars(
      contract=api.Contracts.Futures.TXF.TXFR1,
      start=(datetime.date.today() -datetime.timedelta(days=days)).strftime("%Y-%m-%d"),
      end=datetime.date.today().strftime("%Y-%m-%d"),
      )
  df = pd.DataFrame({**kbars})
  df.ts = pd.to_datetime(df.ts)
  df = df.set_index('ts')
  df.index.name = None
  df = df[['Open', 'High', 'Low', 'Close', 'Volume']]
  df = df.between_time('08:44:00', '13:45:01')
  df_5min = df.resample('5T', label='right', closed='right').agg(
    { 'Open': 'first',
      'High': 'max',
      'Low': 'min',
      'Close': 'last',
      'Volume': 'sum'
    })
  df_5min.dropna(axis=0, inplace=True)
  days += 1

ts = datetime.datetime.now()

while datetime.datetime.now().time() < datetime.time(13, 40):
  time.sleep(1)
  # this place can add stop or limit order
  self_list_positions = order.list_positions(msg_queue['bidask'][-1])
  self_position = 'None' if len(self_list_positions) == 0 else self_list_positions['direction']
  # if self_position == 'Buy':
  #   if msg_queue['bidask'][-1]['ask_price'][0] < (self_list_positions['price'] * 0.999):
  #     order.place_order(msg_queue['bidask'][-1], 'Sell', 'Cover')

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
    df = df.between_time('08:44:00', '13:45:01')
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

      adx_period = 30
      rsi_period = 22
      overbought = 70
      oversold = 30

      self_adx = ta.ADX(df_5min['High'], df_5min['Low'], df_5min['Close'], adx_period)
      self_rsi = ta.RSI(df_5min['Close'], rsi_period)
      self_slope = ta.LINEARREG_SLOPE(df_5min['Close'], rsi_period)
      self_rsi_slope = ta.LINEARREG_SLOPE(self_rsi, rsi_period)

      condition1 = datetime.datetime.now().time() < datetime.time(13, 15)
      condition2 = datetime.datetime.now().time() > datetime.time(9, 15)
      condition3 = datetime.datetime.now().time() >= datetime.time(13, 25)
      condition4 = (df_5min['Close'][-1]>df_5min['Open'][-1]) and (df_5min['Close'][-2]>df_5min['Open'][-2]) and (df_5min['Close'][-3]>df_5min['Open'][-3]) # three up kbars
      condition5 = (df_5min['Close'][-1]<df_5min['Open'][-1]) and (df_5min['Close'][-2]<df_5min['Open'][-2]) and (df_5min['Close'][-3]<df_5min['Open'][-3]) # three down kbars
      condition6 = (self_rsi[-2:] > overbought).all() # rsi overbought
      condition7 = (self_rsi[-2:] < oversold).all() # rsi oversold
      condition8 = self_slope[-1] > 0 # price slope > 0
      condition9 = self_slope[-1] < 0 # price slope < 0
      # condition10 = self_adx[-2:] >= 20 # adx > 20
      condition11 = (self_adx[-2:] < 20).all() # adx < 20
      # condition12 = (self_rsi[-2]<overbought) and (self_rsi[-1]>=overbought) # rsi crossover overbought
      condition13 = (self_rsi[-2]>overbought) and (self_rsi[-1]<=overbought) # rsi crossdown overbought
      # condition14 = (self_rsi[-2]>oversold) and (self_rsi[-1]<=oversold) # rsi crossdown oversold
      condition15 = (self_rsi[-2]<oversold) and (self_rsi[-1]>=oversold) # rsi crossover oversold
      condition16 = (self_adx[-2]<25) and (self_adx[-1]>=25) # adx crossover 25
      condition17 = (self_adx[-2]>25) and (self_adx[-1]<=25) # adx crossdown 25
      condition18 = (self_adx[-2:] >= 30).all() # adx > 30
      # condition19 = self_adx[-2:] < 30 # adx < 30
      condition20 = self_rsi_slope[-1] > 0 # rsi slope > 0
      condition21 = self_rsi_slope[-1] < 0 # rsi slope < 0
      condition22 = (self_adx[-2:] >= 25).all() # adx > 25

      self_list_positions = order.list_positions(msg_queue['bidask'][-1])

      self_position = 'None' if len(self_list_positions) == 0 else self_list_positions['direction']

      
      if self_position == 'None':
        if condition1 and condition2: # 9:15 ~ 13:15
          if condition13 and (condition18 or condition16):
            order.place_order(msg_queue['bidask'][-1], 'Sell', 'New')
          elif condition15 and (condition18 or condition16):
            order.place_order(msg_queue['bidask'][-1], 'Buy', 'New')
          elif self_rsi[-1]>=45 and condition22 and condition8 and condition20:
            order.place_order(msg_queue['bidask'][-1], 'Buy', 'New')
          elif self_rsi[-1]<=55 and condition22 and condition9 and condition21:
            order.place_order(msg_queue['bidask'][-1], 'Sell', 'New')
          elif condition22 and condition9 and condition20:
            order.place_order(msg_queue['bidask'][-1], 'Buy', 'New')
          elif condition22 and condition8 and condition21:
            order.place_order(msg_queue['bidask'][-1], 'Sell', 'New')
          

      elif self_position == 'Buy':
        if condition1 and condition2: # 9:15 ~ 13:15
          if msg_queue['tick'][-1]['close'] >= (self_list_positions['price'] * (1+0.035)): # 3.5% stop profit
            order.place_order(msg_queue['bidask'][-1], 'Sell', 'Cover')
          elif msg_queue['tick'][-1]['close'] <= (self_list_positions['price'] * (1-0.007)): # 0.7% stop loss
            order.place_order(msg_queue['bidask'][-1], 'Sell', 'Cover')
          elif condition6 and (condition11 or condition17):
            order.place_order(msg_queue['bidask'][-1], 'Sell', 'Cover')
            order.place_order(msg_queue['bidask'][-1], 'Sell', 'New')
          elif self_rsi[-1]<=55 and condition22 and condition9 and condition21:
            order.place_order(msg_queue['bidask'][-1], 'Sell', 'Cover')
            order.place_order(msg_queue['bidask'][-1], 'Sell', 'New')
          elif condition5:
            order.place_order(msg_queue['bidask'][-1], 'Sell', 'Cover')
            if condition22:
              order.place_order(msg_queue['bidask'][-1], 'Sell', 'New')
        elif condition3:
          order.place_order(msg_queue['bidask'][-1], 'Sell', 'Cover')
      
      elif self_position == 'Sell':
        if condition1 and condition2: # 9:15 ~ 13:15
          if msg_queue['tick'][-1]['close'] <= (self_list_positions['price'] * (1-0.035)): # 3.5% stop profit
            order.place_order(msg_queue['bidask'][-1], 'Buy', 'Cover')
          elif msg_queue['tick'][-1]['close'] >= (self_list_positions['price'] * (1+0.007)): # 0.7% stop loss
            order.place_order(msg_queue['bidask'][-1], 'Buy', 'Cover')
          elif condition7 and (condition11 or condition17):
            order.place_order(msg_queue['bidask'][-1], 'Buy', 'Cover')
            order.place_order(msg_queue['bidask'][-1], 'Buy', 'New')
          elif self_rsi[-1]>45 and condition22 and condition8 and condition20:
            order.place_order(msg_queue['bidask'][-1], 'Buy', 'Cover')
            order.place_order(msg_queue['bidask'][-1], 'Buy', 'New')
          elif condition4:
            order.place_order(msg_queue['bidask'][-1], 'Buy', 'Cover')
            if condition22:
              order.place_order(msg_queue['bidask'][-1], 'Buy', 'New')
        elif condition3:
          order.place_order(msg_queue['bidask'][-1], 'Buy', 'Cover')

api.quote.unsubscribe(
  contract,
  quote_type=sj.constant.QuoteType.BidAsk,
  version=sj.constant.QuoteVersion.v1
)

api.quote.unsubscribe(
  contract,
  quote_type = sj.constant.QuoteType.Tick,
  version = sj.constant.QuoteVersion.v1,
)

api.logout()
