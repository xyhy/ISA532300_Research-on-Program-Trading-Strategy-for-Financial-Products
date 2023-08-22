import shioaji as sj
import pysimulation

from shioaji import TickFOPv1, BidAskFOPv1, Exchange
from collections import defaultdict, deque

import datetime
import pytz

import pandas as pd

api = sj.Shioaji(simulation=True)
accounts = api.login(
  api_key="",     # done
  secret_key=""   # done
)
order = pysimulation.order('108062213') # done


# 五檔報價
contract = min(
  [
    x for x in api.Contracts.Futures.TXF
    if x.code[-2:] not in ["R1", "R2"]
  ],
  key=lambda x: x.delivery_date
)


msg_queue = defaultdict(deque)
api.set_context(msg_queue)

# In order to use context, set bind=True
@api.on_tick_fop_v1(bind=True)
def quote_callback(self, exchange:Exchange, tick:TickFOPv1):
  # append quote to message queue
  self['tick'].append(tick)
  print(f"Exchange: {exchange}, Tick: {tick}")

# In order to use context, set bind=True
@api.on_bidask_fop_v1(bind=True)
def quote_callback(self, exchange:Exchange, bidask:BidAskFOPv1):
  # append quote to message queue
  self['bidask'].append(bidask)
  print(f"Exchange: {exchange}, Bidask: {bidask}")

api.quote.subscribe(
  contract,
  quote_type = sj.constant.QuoteType.Tick,
  version = sj.constant.QuoteVersion.v1,
)

api.quote.subscribe(
  contract,
  quote_type = sj.constant.QuoteType.BidAsk,
  version = sj.constant.QuoteVersion.v1
)


while True:
    if datetime.datetime.now(pytz.timezone('ROC')).time() >= datetime.time(8, 45, 10):
        order.place_order(msg_queue['bidask'][-1], 'Buy', 'New')
        # 開盤買進
        break
    else:
        continue

while True:
    if datetime.datetime.now(pytz.timezone('ROC')).time() >= datetime.time(13, 30, 0):
      # 收盤賣出
        if(len(order.list_trades()) != 0):
            order.place_order(msg_queue['bidask'][-1], 'Sell', 'Cover')
            history = pd.DataFrame(order.list_trades())
            history.to_csv('../history.csv', index=False)
            break


api.quote.unsubscribe(
    contract,
    quote_type = sj.constant.QuoteType.Tick,
    version = sj.constant.QuoteVersion.v1,
)

api.quote.unsubscribe(
    contract, 
    quote_type = sj.constant.QuoteType.BidAsk,
    version = sj.constant.QuoteVersion.v1
)
api.logout()