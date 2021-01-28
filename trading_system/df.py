import pandas as pd
import collections
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

import threading
import time
from Program import *

# Count the total symbols that are requested market data
total_requests = 0
id = 1
order_id = 1
sleep_interval = 0.1
dollars = 1000
trading_hour = 9
trading_minute_start = 31
trading_minute_end = 46
liquidate_hour = 15
liquidate_minute = 0
long_short_position_number_limit = 5
num_order_limit = 1

import datetime
# BDay is business day, not birthday...
from pandas.tseries.offsets import BDay

today = datetime.datetime.today()
yesterday = today - BDay(1)
yesterday = yesterday.strftime('%Y%m%d')

df_params = pd.read_csv('/Users/d567533/DF/params.csv')
df_params['all_params'] = df_params[['const', 'x1', 'x2']].values.tolist()
params_mp = df_params.set_index('symbol')['all_params'].to_dict()
df_params0 = pd.read_csv('/Users/d567533/DF/params0.csv')
df_params0['all_params'] = df_params0[['const', 'x1']].values.tolist()
params_mp0 = df_params0.set_index('symbol')['all_params'].to_dict()

id_equity_info_mp = {}
symbols = list(df_params.symbol)
symbols.insert(0, 'SPY')
placed_symbols = {}
open_orders = {}


class TradingAPI(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.data = []  # Initialize variable to store candle
        self.positions = {}
        self.yesterday_return = {}
        self.predictions = {}
        self.market_price = {}

    def historicalData(self, reqId, bar):

        if bar.date == yesterday:
            id_equity_info_mp[reqId].yesterday_close = bar.close
            print(
                f'{id_equity_info_mp[reqId].symbol} yesterday close '
                f'{id_equity_info_mp[reqId].yesterday_close}')

    @iswrapper
    # ! [position]
    def position(self, account: str, contract: Contract, position: float,
                 avgCost: float):
        super().position(account, contract, position, avgCost)
        # print("Position.", "Account:", account, "Symbol:", contract.symbol, "SecType:",
        #       contract.secType, "Currency:", contract.currency,
        #       "Position:", position, "Avg cost:", avgCost)
        if contract.SecType == 'STK':
            self.positions[contract.symbol] = position

    # ! [position]

    @iswrapper
    # ! [tickprice]
    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float,
                  attrib: TickAttrib):
        super().tickPrice(reqId, tickType, price, attrib)
        if price != -1 and reqId in id_equity_info_mp:
            if tickType == TickTypeEnum.BID:
                id_equity_info_mp[reqId].bid = price
            if tickType == TickTypeEnum.ASK:
                id_equity_info_mp[reqId].ask = price
            if tickType == TickTypeEnum.LAST:
                id_equity_info_mp[reqId].last = price

    # ! [tickprice]

    # def marketDataType(self, reqId: TickerId, marketDataType: int):
    #     super().marketDataType(reqId, marketDataType)
    #     print("MarketDataType. ReqId:", reqId, "Type:", marketDataType)
    @iswrapper
    # ! [orderstatus]
    def orderStatus(self, orderId: OrderId, status: str, filled: float,
                    remaining: float, avgFillPrice: float, permId: int,
                    parentId: int, lastFillPrice: float, clientId: int,
                    whyHeld: str, mktCapPrice: float):
        super().orderStatus(orderId, status, filled, remaining,
                            avgFillPrice, permId, parentId, lastFillPrice,
                            clientId, whyHeld, mktCapPrice)
        print("OrderStatus. Id:", orderId, "Status:", status, "Filled:",
              filled,
              "Remaining:", remaining, "AvgFillPrice:", avgFillPrice,
              "PermId:", permId, "ParentId:", parentId, "LastFillPrice:",
              lastFillPrice, "ClientId:", clientId, "WhyHeld:",
              whyHeld, "MktCapPrice:", mktCapPrice)
        if remaining == 0:
            del open_orders[orderId]

    # ! [orderstatus]

    @iswrapper
    def cancelOrder(self, orderId: OrderId):
        super().cancelOrder(orderId)
        del open_orders[orderId]


class equity_info:
    def __init__(self, symbol):
        self.symbol = symbol
        self.last = -2
        self.ask = -2
        self.bid = -2
        self.prev_close = -2
        self.predict = 0


app = TradingAPI()


def run_loop():
    global id_equity_info_mp
    app.run()


def compute():
    global id_equity_info_mp
    while True:
        print("Computing thread")
        time.sleep(sleep_interval)
        print(f'Total requested symbol number {total_requests}')
        print(f'Total symbols in the map {len(id_equity_info_mp)}')
        if 1 in id_equity_info_mp:
            assert (id_equity_info_mp[1].symbol == 'SPY')
            if id_equity_info_mp[1].last > 0:

                spy_return = id_equity_info_mp[1].last / id_equity_info_mp[
                    1].prev_close - 1
                print(f"SPY return is {spy_return}")
                for id in list(id_equity_info_mp):
                    symbol = id_equity_info_mp[id].symbol
                    if id != 1:
                        pricing = params_mp0[symbol][0] + params_mp0[symbol][
                            1] * spy_return
                        pricing_error = (
                                id_equity_info_mp[id].last /
                                id_equity_info_mp[id].prev_close
                                - 1 - pricing
                        )
                        id_equity_info_mp[id].predict = (
                                params_mp[symbol][0] + params_mp[symbol][
                            2] * pricing_error
                        )

        if len(id_equity_info_mp) >= 91:
            id_equity_info_mp = (
                collections.OrderedDict(sorted(
                    id_equity_info_mp.items(),
                    key=lambda item: item[1].predict
                ))
            )
            for i in range(45, len(id_equity_info_mp)):
                print("Deleting ...")
                key = list(id_equity_info_mp.keys())[i]
                if (
                        id_equity_info_mp[key] != 'SPY' and
                        id_equity_info_mp.last != -2
                ):
                    print(f'delete symbol {id_equity_info_mp[key].symbol} '
                          f' last price {id_equity_info_mp[key].last}'
                          f' predict {id_equity_info_mp[key].predict}')
                    del id_equity_info_mp[key]
                    app.cancelMktData(key)
                    break


def request_market_data():
    global id_equity_info_mp, total_requests, id, symbols
    while True:
        print("Request market data thread")
        time.sleep(sleep_interval)

        requested_symbols = [
            value.symbol for value in id_equity_info_mp.values()
        ]
        symbols_valid_last = [
            value.symbol for value in id_equity_info_mp.values()
            if value.last > 0
        ]
        print(
            f'length of all symbols {len(symbols)}\n'
            f'Total live requested symbols number {len(id_equity_info_mp)}\n'
            f'Positive last symbols number {len(symbols_valid_last)}\n'
            f'id {id}\n'

        )

        if id <= len(symbols):
            print(f'next symbol {symbols[id - 1]}\n')
            symbol = symbols[id - 1]
            if symbol not in requested_symbols:  # and len(id_equity_info_mp) <= 80:
                contract = Contract()
                contract.symbol = symbol
                contract.secType = 'STK'
                contract.exchange = 'SMART'
                contract.currency = 'USD'
                app.reqHistoricalData(
                    id, contract, '', '2 D', '1 day', 'TRADES', 1, 1, False, []
                )

                app.reqMktData(id, contract, '', False, False, [])
                total_requests += 1
                id_equity_info_mp[id] = equity_info(symbol)
                id += 1


def count_long_short_positions():
    long_position_number = 0
    short_position_number = 0
    for position in app.positions.values():
        if position > 0:
            long_position_number += 1
        elif position < 0:
            short_position_number -= 1
    return long_position_number, short_position_number


def get_num_buy_and_sell_orders():
    global open_orders
    num_sell_orders = 0
    num_buy_orders = 0
    for id, order in open_orders.items():
        if order.action == 'BUY':
            num_buy_orders += 1
        else:
            num_sell_orders += 1

    return num_buy_orders, num_sell_orders


def cancel_other_orders(symbol, action, limit_price):
    """Cancel not wanted orders.

    Cancel the same side orders that symbols are different from the
    input argument. For orders with the same symbol, cancel far orders.
    return True if the wanted order already existed."""
    global app
    for id, order in open_orders.items():
        if order.action == action:
            if placed_symbols[id] != symbol:
                app.cancelOrder(id)
            else:
                if action == 'BUY' and order.lmtPrice < limit_price:
                    app.cancelOrder(id)

                if action == 'SELL' and order.lmtPrice > limit_price:
                    app.cancelOder(id)



def liquidate_positions():
    global app, order_id, num_order_limit
    while True:
        time.sleep(1)
        now = datetime.datetime.now()
        if now.hour < liquidate_hour:
            continue

        long_position_number, short_position_number = (
            count_long_short_positions()
        )
        num_sell_orders, num_buy_orders = get_num_buy_and_sell_orders()
        for symbol, position in app.positions.items():

            for value in id_equity_info_mp.values():
                if (
                        value.symbol == symbol and
                        value.bid > 0 and value.ask > 0 and value.last > 0
                ):

                    if (
                            position < 0 and
                            long_position_number <= short_position_number and
                            num_buy_orders < num_order_limit
                    ):
                        place_order(symbol, "BUY", value.last,
                                    value.bid)

                    if (
                            position > 0 and
                            long_position_number >= short_position_number and
                            num_sell_orders < num_order_limit
                    ):
                        place_order(symbol, "SELL", value.last,
                                    value.ask)


def risk_check_position(value: equity_info, action: str):
    long_position_number, short_position_number = (
        count_long_short_positions()
    )
    valid = (
            value.symbol not in app.positions and value.last > 0
            and value.bid > 0 and value.ask > 0 and
            value.symbol not in placed_symbols
    )

    if action == 'SELL':
        return (
            valid and
            long_position_number >= short_position_number and
            short_position_number < long_short_position_number_limit
        )
    if action == 'BUY':
        return (
            valid and
            long_position_number <= short_position_number and
            long_position_number < long_short_position_number_limit
        )


def manage_orders():
    global id_equity_info_mp, app, order_id, placed_symbols
    while True:
        print("Place order thread: ")
        time.sleep(sleep_interval)
        now = datetime.datetime.now()
        if (total_requests > 300 and len(id_equity_info_mp) >= 80 and
                now.hour == trading_hour and
                trading_minute_start <= now.minute <= trading_minute_end
        ):
            id_equity_info_mp = (
                collections.OrderedDict(sorted(
                    id_equity_info_mp.items(),
                    key=lambda item: item[1].predict
                ))
            )
            for key in list(id_equity_info_mp):
                try:
                    value = id_equity_info_mp[key]
                    if risk_check_position(value, 'SELL'):
                        print(
                            f'Smallest predict \n'
                            f'{value.symbol} predict {value.predict}\n'
                            f'{value.symbol} bid {value.bid}\n'
                            f'{value.symbol} ask {value.ask}\n'
                            f'{value.symbol} last {value.last}\n'

                        )
                        cancel_other_orders(value.symbol, 'SELL', value.ask)
                        place_order(value.symbol, "SELL", value.last, value.ask)
                        break
                except KeyError:
                    pass

            for key in reversed(list(id_equity_info_mp)):
                try:
                    value = id_equity_info_mp[key]
                    if risk_check_position(value, 'BUY'):
                        print(
                            f'Largest predict \n'
                            f'{value.symbol} predict {value.predict}\n'
                            f'{value.symbol} bid {value.bid}\n'
                            f'{value.symbol} ask {value.ask}\n'
                            f'{value.symbol} last {value.last}\n'
                        )
                        cancel_other_orders(value.symbol, 'BUY', value.bid)
                        place_order(value.symbol, "BUY", value.last, value.bid)
                        break
                except KeyError:
                    pass


def place_order(symbol, action, last_price, limit_price):
    global app, placed_symbols, order_id, dollars, open_orders
    for id, order in open_orders:
        if (
            placed_symbols[id] == symbol and
            order.action == action and
            order.lmtPrice == limit_price
        ):
            return
    contract = Contract()
    contract.symbol = symbol
    contract.secType = 'STK'
    contract.exchange = 'SMART'
    contract.currency = 'USD'
    order = Order()
    order.action = action
    order.tif = "AUC"
    order.orderType = "LMT"
    order.totalQuantity = int(round(dollars / last_price))
    order.lmtPrice = limit_price
    app.placeOrder(order_id, contract, order)
    open_orders[order_id] = order
    placed_symbols[order_id] = contract.symbol
    order_id += 1


def cancel_far_orders():
    while True:
        time.sleep(sleep_interval)

        for id, order in open_orders.items():
            for value in id_equity_info_mp.values():
                if value.symbol == placed_symbols[id]:
                    if (
                        (order.action == 'BUY' and value.bid > 0 and
                         order.lmtPrice < value.bid * .997) or
                        value.bid < 0
                    ):
                        app.cancelOrder(id)
                        break

                    if (
                        (order.action == 'SELL' and value.ask > 0 and
                         order.lmtPrice > value.ask / 0.997) or
                        value.ask < 0
                    ):
                        app.cancelOrder(id)
                        break


app.connect('127.0.0.1', 7497, 123)
time.sleep(sleep_interval)
app.reqPositions()

# Start the socket in a thread
api_thread = threading.Thread(target=run_loop, daemon=True)
api_thread.start()

marketdata_thread = threading.Thread(target=request_market_data)
marketdata_thread.start()

compute_thread = threading.Thread(target=compute)
compute_thread.start()

cancel_far_order_thread = threading.Thread(target=cancel_far_orders)
cancel_far_order_thread.start()

liquidate_positions_thread = threading.Thread(target=liquidate_positions)
liquidate_positions_thread.start()

manage_orders_thread = threading.Thread(target=manage_orders)
manage_orders_thread.start()

# Join threads
api_thread.join()
compute_thread.join()
app.reqPositions()
app.reqOpenOrders()
cancel_far_order_thread.join()
manage_orders_thread.join()
liquidate_positions_thread.join()

# app.disconnect()
print("Program ends!!!")
