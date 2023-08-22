import shioaji as sj
from collections import defaultdict, deque
from shioaji import BidAskFOPv1, Exchange
import time
import os
import pandas as pd
from math import ceil


class pytrader:

    def __init__(self, strategy, api_key, secret_key, ca_path=None, ca_passwd=None, person_id=None, simulation=True):

        self.strategy = strategy
        self.simulation = simulation
        global api
        api = sj.Shioaji()
        api.login(
            api_key=api_key,
            secret_key=secret_key
        )
        if(simulation != True):
            api.activate_ca(
                ca_path=ca_path,
                ca_passwd=ca_passwd,
                person_id=person_id,
            )

        if os.path.isfile(f'{self.strategy}_orders.csv') == False:
            df = pd.DataFrame(
                columns=['entry_time', 'code', 'size', 'entry_price', 'fee', 'tax'])
            df.set_index('entry_time', inplace=True)
            df.to_csv(f'{self.strategy}_orders.csv')

        self.orders = pd.read_csv(
            f'{self.strategy}_orders.csv', index_col='entry_time')
        self.orders.index = pd.to_datetime(
            self.orders.index, format='%Y-%m-%d %H:%M:%S')
        self.orders = self.orders.reset_index()
        self.orders['entry_time'] = self.orders['entry_time'].dt.to_pydatetime()
        self.orders = self.orders.to_dict('records')

        if os.path.isfile(f'{self.strategy}_trades.csv') == False:
            df = pd.DataFrame(
                columns=['entry_time', 'exit_time', 'code', 'size', 'entry_price', 'exit_price', 'fee', 'tax', 'pnl'])
            df.set_index('entry_time', inplace=True)
            df.to_csv(f'{self.strategy}_trades.csv')

        self.trades = pd.read_csv(
            f'{self.strategy}_trades.csv', index_col='entry_time')
        self.trades.index = pd.to_datetime(
            self.trades.index, format='%Y-%m-%d %H:%M:%S')
        self.trades = self.trades.reset_index()
        self.trades['entry_time'] = self.trades['entry_time'].dt.to_pydatetime()

        self.trades = self.trades.to_dict('records')

    def contract(self, code):

        self._contract = min(
            [
                x for x in getattr(api.Contracts.Futures, code)
                if x.code[-2:] not in ["R1", "R2"]
            ],
            key=lambda x: x.delivery_date
        )

        global msg_queue
        msg_queue = defaultdict(deque)
        api.set_context(msg_queue)

        @api.on_bidask_fop_v1(bind=True)
        def quote_callback(self, exchange: Exchange, bidask: BidAskFOPv1):
            # append quote to message queue
            self['bidask'].append(bidask)

        api.quote.subscribe(
            self._contract,
            quote_type=sj.constant.QuoteType.BidAsk,
            version=sj.constant.QuoteVersion.v1
        )

        time.sleep(1.5)


    def order(self, price_type, order_type, octype):

        self._price_type = price_type
        self._order_type = order_type
        self._octype = octype

    def buy(self, size):

        if(self.simulation == False):
            global api
            order = api.Order(
                action=sj.constant.Action.Buy,
                price=round(msg_queue['bidask'][-1]['ask_price'][0]),
                quantity=size,
                price_type=getattr(
                    sj.constant.FuturesPriceType, self._price_type),
                order_type=getattr(sj.constant.OrderType, self._order_type),
                octype=getattr(sj.constant.FuturesOCType, self._octype),
                account=api.futopt_account
            )
            trade = api.place_order(self._contract, order)

        orders = pd.read_csv(f'{self.strategy}_orders.csv', index_col=None)
        orders = pd.concat([orders, pd.DataFrame.from_dict({
            'entry_time': [msg_queue['bidask'][-1]['datetime'].strftime('%Y-%m-%d %H:%M:%S')],
            'code': [self._contract.code],
            'size': [size],
            'entry_price': [round(msg_queue['bidask'][-1]['ask_price'][0])],
            'fee': [50],
            'tax': [round(round(msg_queue['bidask'][-1]['ask_price'][0]) * size * 0.004)]
        })], ignore_index=True)

        orders['entry_time'] = pd.to_datetime(
            orders['entry_time'], format='%Y-%m-%d %H:%M:%S')
        orders.set_index('entry_time', inplace=True)
        orders.to_csv(f'{self.strategy}_orders.csv')

        self.orders = orders.reset_index()
        self.orders['entry_time'] = self.orders['entry_time'].dt.to_pydatetime()
        self.orders = self.orders.to_dict('records')

        trades = pd.read_csv(f'{self.strategy}_trades.csv', index_col=None)
        trades['entry_time'] = pd.to_datetime(
            trades['entry_time'], format='%Y-%m-%d %H:%M:%S')
        if(len(trades) == 0):
            trades = pd.concat([trades, pd.DataFrame.from_dict({
                'entry_time': [self.orders[-1]['entry_time']],
                'exit_time': [None],
                'code': [self.orders[-1]['code']],
                'size': [size],
                'entry_price': [self.orders[-1]['entry_price']],
                'exit_price': [None],
                'fee': [50],
                'tax': [self.orders[-1]['tax']],
                'pnl': [None]
            })], ignore_index=True)

        elif(orders['size'].sum() > 0):
            if(pd.isna(trades.loc[trades.index[-1], 'exit_time'])):
                if(trades.loc[trades.index[-1], 'size'] < 0):
                    trades.loc[trades.index[-1],
                               'exit_time'] = self.orders[-1]['entry_time']
                    trades.loc[trades.index[-1],
                               'exit_price'] = self.orders[-1]['entry_price']
                    trades.loc[trades.index[-1], 'fee'] += 50
                    trades.loc[trades.index[-1], 'tax'] += ceil(
                        self.orders[-1]['tax'] * (-trades.iloc[-1]['size'] / size))
                    trades.loc[trades.index[-1], 'pnl'] = ((trades.loc[trades.index[-1], 'entry_price'] - trades.loc[trades.index[-1], 'exit_price'])
                                                           * 200 * -trades.iloc[-1]['size']) - trades.loc[trades.index[-1], 'fee'] - trades.loc[trades.index[-1], 'tax']
                    trades = pd.concat([trades, pd.DataFrame.from_dict({
                        'entry_time': [self.orders[-1]['entry_time']],
                        'exit_time': [None],
                        'code': [self.orders[-1]['code']],
                        'size': [size + trades.iloc[-1]['size']],
                        'entry_price': [self.orders[-1]['entry_price']],
                        'exit_price': [None],
                        'fee': [50],
                        'tax': [ceil(self.orders[-1]['tax'] * ((size + trades.iloc[-1]['size']) / size))],
                        'pnl': [None]
                    })], ignore_index=True)
                else:
                    trades.loc[trades.index[-1], 'entry_price'] = ceil((trades.iloc[-1]['entry_price'] * (trades.iloc[-1]['size'] / (
                        trades.iloc[-1]['size'] + size))) + (self.orders[-1]['entry_price'] * (size / (trades.iloc[-1]['size'] + size))))
                    trades.loc[trades.index[-1], 'size'] += size
                    trades.loc[trades.index[-1], 'fee'] += 50
                    trades.loc[trades.index[-1],
                               'tax'] += self.orders[-1]['tax']
            else:
                # new buy position
                trades = pd.concat([trades, pd.DataFrame.from_dict({
                    'entry_time': [self.orders[-1]['entry_time']],
                    'exit_time': [None],
                    'code': [self.orders[-1]['code']],
                    'size': [size],
                    'entry_price': [self.orders[-1]['entry_price']],
                    'exit_price': [None],
                    'fee': [50],
                    'tax': [self.orders[-1]['tax']],
                    'pnl': [None]
                })], ignore_index=True)

        else:

            if(orders['size'].sum() == 0):

                trades.loc[trades.index[-1],
                           'exit_time'] = self.orders[-1]['entry_time']
                trades.loc[trades.index[-1],
                           'exit_price'] = self.orders[-1]['entry_price']
                trades.loc[trades.index[-1], 'fee'] += 50
                trades.loc[trades.index[-1], 'tax'] += self.orders[-1]['tax']
                trades.loc[trades.index[-1], 'pnl'] = ((trades.loc[trades.index[-1], 'entry_price'] - trades.loc[trades.index[-1], 'exit_price'])
                                                       * 200 * size) - trades.loc[trades.index[-1], 'fee'] - trades.loc[trades.index[-1], 'tax']
            else:
                # close a part of the position
                trades = pd.concat([trades, pd.DataFrame.from_dict({
                    'entry_time': [(trades.iloc[-1]['entry_time'] + pd.Timedelta(1, "s")).strftime('%Y-%m-%d %H:%M:%S')],
                    'exit_time': [None],
                    'code': [self.orders[-1]['code']],
                    'size': [trades.iloc[-1]['size'] + size],
                    'entry_price': [trades.iloc[-1]['entry_price']],
                    'exit_price': [None],
                    'fee': [50],
                    'tax': [trades.iloc[-1]['tax']],
                    'pnl': [None]
                })], ignore_index=True)
                trades.loc[trades.index[-2],
                           'exit_time'] = self.orders[-1]['entry_time']
                trades.loc[trades.index[-2], 'size'] = -size
                trades.loc[trades.index[-2],
                           'exit_price'] = self.orders[-1]['entry_price']
                trades.loc[trades.index[-2], 'tax'] = self.orders[-1]['tax']
                trades.loc[trades.index[-2], 'pnl'] = ((trades.loc[trades.index[-2], 'entry_price'] - trades.loc[trades.index[-2], 'exit_price'])
                                                       * 200 * size) - trades.loc[trades.index[-2], 'fee'] - trades.loc[trades.index[-2], 'tax']

        trades['entry_time'] = pd.to_datetime(
            trades['entry_time'], format='%Y-%m-%d %H:%M:%S')
        trades.set_index('entry_time', inplace=True)
        trades.to_csv(f'{self.strategy}_trades.csv')

        self.trades = trades.reset_index()
        self.trades['entry_time'] = self.trades['entry_time'].dt.to_pydatetime()
        self.trades = self.trades.to_dict('records')

    def sell(self, size):

        if(self.simulation == False):
            global api
            order = api.Order(
                action=sj.constant.Action.Sell,
                price=round(msg_queue['bidask'][-1]['bid_price'][0]),
                quantity=size,
                price_type=getattr(
                    sj.constant.FuturesPriceType, self._price_type),
                order_type=getattr(sj.constant.OrderType, self._order_type),
                octype=getattr(sj.constant.FuturesOCType, self._octype),
                account=api.futopt_account
            )
            trade = api.place_order(self._contract, order)

        orders = pd.read_csv(f'{self.strategy}_orders.csv', index_col=None)
        orders = pd.concat([orders, pd.DataFrame.from_dict({
            'entry_time': [msg_queue['bidask'][-1]['datetime'].strftime('%Y-%m-%d %H:%M:%S')],
            'code': [self._contract.code],
            'size': [-size],
            'entry_price': [round(msg_queue['bidask'][-1]['bid_price'][0])],
            'fee': [50],
            'tax': [round(round(msg_queue['bidask'][-1]['bid_price'][0]) * size * 0.004)]
        })], ignore_index=True)

        orders['entry_time'] = pd.to_datetime(
            orders['entry_time'], format='%Y-%m-%d %H:%M:%S')
        orders.set_index('entry_time', inplace=True)
        orders.to_csv(f'{self.strategy}_orders.csv')

        self.orders = orders.reset_index()
        self.orders['entry_time'] = self.orders['entry_time'].dt.to_pydatetime()
        self.orders = self.orders.to_dict('records')

        trades = pd.read_csv(f'{self.strategy}_trades.csv', index_col=None)
        trades['entry_time'] = pd.to_datetime(
            trades['entry_time'], format='%Y-%m-%d %H:%M:%S')
        if(len(trades) == 0):
            trades = pd.concat([trades, pd.DataFrame.from_dict({
                'entry_time': [self.orders[-1]['entry_time']],
                'exit_time': [None],
                'code': [self.orders[-1]['code']],
                'size': [-size],
                'entry_price': [self.orders[-1]['entry_price']],
                'exit_price': [None],
                'fee': [50],
                'tax': [self.orders[-1]['tax']],
                'pnl': [None]
            })], ignore_index=True)
        elif(orders['size'].sum() < 0):
            if(pd.isna(trades.loc[trades.index[-1], 'exit_time'])):
                if(trades.loc[trades.index[-1], 'size'] > 0):
                    trades.loc[trades.index[-1],
                               'exit_time'] = self.orders[-1]['entry_time']
                    trades.loc[trades.index[-1],
                               'exit_price'] = self.orders[-1]['entry_price']
                    trades.loc[trades.index[-1], 'fee'] += 50
                    trades.loc[trades.index[-1], 'tax'] += ceil(
                        self.orders[-1]['tax'] * (trades.iloc[-1]['size'] / size))
                    trades.loc[trades.index[-1], 'pnl'] = ((trades.loc[trades.index[-1], 'exit_price'] - trades.loc[trades.index[-1], 'entry_price'])
                                                           * 200 * trades.iloc[-1]['size']) - trades.loc[trades.index[-1], 'fee'] - trades.loc[trades.index[-1], 'tax']
                    trades = pd.concat([trades, pd.DataFrame.from_dict({
                        'entry_time': [self.orders[-1]['entry_time']],
                        'exit_time': [None],
                        'code': [self.orders[-1]['code']],
                        'size': [trades.iloc[-1]['size'] - size],
                        'entry_price': [self.orders[-1]['entry_price']],
                        'exit_price': [None],
                        'fee': [50],
                        'tax': [ceil(self.orders[-1]['tax'] * ((size - trades.iloc[-1]['size']) / size))],
                        'pnl': [None]
                    })], ignore_index=True)
                else:
                    trades.loc[trades.index[-1], 'entry_price'] = ceil((trades.iloc[-1]['entry_price'] * (trades.iloc[-1]['size'] / (
                        trades.iloc[-1]['size'] - size))) + (self.orders[-1]['entry_price'] * (-size / (trades.iloc[-1]['size'] - size))))
                    trades.loc[trades.index[-1], 'size'] -= size
                    trades.loc[trades.index[-1], 'fee'] += 50
                    trades.loc[trades.index[-1],
                               'tax'] += self.orders[-1]['tax']
            else:
                trades = pd.concat([trades, pd.DataFrame.from_dict({
                    'entry_time': [self.orders[-1]['entry_time']],
                    'exit_time': [None],
                    'code': [self.orders[-1]['code']],
                    'size': [-size],
                    'entry_price': [self.orders[-1]['entry_price']],
                    'exit_price': [None],
                    'fee': [50],
                    'tax': [self.orders[-1]['tax']],
                    'pnl': [None]
                })], ignore_index=True)
        else:
            if(orders['size'].sum() == 0):
                trades.loc[trades.index[-1],
                           'exit_time'] = self.orders[-1]['entry_time']
                trades.loc[trades.index[-1],
                           'exit_price'] = self.orders[-1]['entry_price']
                trades.loc[trades.index[-1], 'fee'] += 50
                trades.loc[trades.index[-1], 'tax'] += self.orders[-1]['tax']
                trades.loc[trades.index[-1], 'pnl'] = ((trades.loc[trades.index[-1], 'exit_price'] - trades.loc[trades.index[-1], 'entry_price'])
                                                       * 200 * size) - trades.loc[trades.index[-1], 'fee'] - trades.loc[trades.index[-1], 'tax']
            else:
                trades = pd.concat([trades, pd.DataFrame.from_dict({
                    'entry_time': [(trades.iloc[-1]['entry_time'] + pd.Timedelta(1, "s")).strftime('%Y-%m-%d %H:%M:%S')],
                    'exit_time': [None],
                    'code': [self.orders[-1]['code']],
                    'size': [trades.iloc[-1]['size'] - size],
                    'entry_price': [trades.iloc[-1]['entry_price']],
                    'exit_price': [None],
                    'fee': [50],
                    'tax': [trades.iloc[-1]['tax']],
                    'pnl': [None]
                })], ignore_index=True)
                trades.loc[trades.index[-2],
                           'exit_time'] = self.orders[-1]['entry_time']
                trades.loc[trades.index[-2], 'size'] = size
                trades.loc[trades.index[-2],
                           'exit_price'] = self.orders[-1]['entry_price']
                trades.loc[trades.index[-2], 'tax'] = self.orders[-1]['tax']
                trades.loc[trades.index[-2], 'pnl'] = ((trades.loc[trades.index[-2], 'exit_price'] - trades.loc[trades.index[-2], 'entry_price'])
                                                       * 200 * size) - trades.loc[trades.index[-2], 'fee'] - trades.loc[trades.index[-2], 'tax']

        trades['entry_time'] = pd.to_datetime(
            trades['entry_time'], format='%Y-%m-%d %H:%M:%S')
        trades.set_index('entry_time', inplace=True)
        trades.to_csv(f'{self.strategy}_trades.csv')

        self.trades = trades.reset_index()
        self.trades['entry_time'] = self.trades['entry_time'].dt.to_pydatetime()
        self.trades = self.trades.to_dict('records')
            
    def position(self):
        
        position = {}
        
        if(sum(item['size'] for item in self.orders) != 0):
            position = {
                'size': sum(item['size'] for item in self.orders),
            }
            position['is_long'] = True if position['size'] > 0 else False
            position['is_short'] = True if position['size'] < 0 else False

            if position['size'] > 0:
                position['pnl'] = round(((float(msg_queue['bidask'][-1]['bid_price'][0])*0.99998 - self.trades[-1]
                                               ['entry_price'])*200*position['size']) - 100 - self.trades[-1]['tax'])
                position['pl_pct'] = (
                    (float(msg_queue['bidask'][-1]['bid_price'][0])*0.99998 - self.trades[-1]['entry_price']*1.00002) / (self.trades[-1]['entry_price']))
            else:
                position['pnl'] = round(((self.trades[-1]['entry_price'] - float(msg_queue['bidask'][-1]
                                                                                      ['ask_price'][0])*1.00002)*200*-position['size']) - 100 - self.trades[-1]['tax'])
                position['pl_pct'] = (
                    (self.trades[-1]['entry_price']*0.99998 - float(msg_queue['bidask'][-1]['bid_price'][0])*1.00002) / (self.trades[-1]['entry_price']))
            
        return position

    def list_trades(self):

        if(self.simulation == False):
            global api
            api.update_status(api.futopt_account)
            return api.list_trades()
        else:
            print('use self.orders instead')
            return

    def list_positions(self):

        if(self.simulation == False):
            global api
            return api.list_positions(api.futopt_account)
        else:
            print('use position instead')
            return
