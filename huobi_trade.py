from datetime import datetime
from threading import Lock, Thread
import pytz
import datetime
from config import config
from requests import exceptions
from huobi_socket_spot import HBSpot_Websocket
from huobi_socket_CoinFuture import HBCoinFuture_Websocket

import numpy as np
import sys, os

from utils import Utils
from dingtalkchatbot.chatbot import DingtalkChatbot
import time
import json

import math





def time_gtc8():
    """
    转化成日期格式
    """
    t = datetime.fromtimestamp(int(time.time()), pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S %Z%z')
    return t.replace(" ", "_")







class DataHuobi:

    def __init__(self):
        self.symbols_table = {}
        self.symbols_table['times'] = 0
        for symbol in config.symbols:

            self.symbols_table[symbol] = {"future": None, "spot": None, "diff_time": time.time(),
                                        "open_premium": None, "close_premium": None,"quantity": None, "price": None,
                                        "update_time_f": time.time(), "update_time_s": time.time(), "update_time":time.time(), "times": 0, "flag":2}
        self.utils = Utils()
        self.lock = Lock()
        self.dingding = DingtalkChatbot(config.webhook)
        self.NN = time.time()
        self.NN_batch = time.time()
        self.count_error = 0
    def init_monitor(self):
        #N = [0, time.time()]
        def process_spot_message(node):
            '''
            处理现货tick数据
            '''

            symbol = node['symbol'][:-4]
            self.symbols_table[symbol]["spot"] = node
            self.symbols_table[symbol]['spot']['ws_delay'] = abs(self.symbols_table[symbol]['spot']['time_receive'] - self.symbols_table[symbol]['spot']['time'] - self.utils.time_sync_spot)
            self.symbols_table[symbol]["update_time_s"] = node['time']
            self.symbols_table[symbol]['update_time'] = node['time']
            self.symbols_table['times'] += 1
            if self.symbols_table['times'] == 100000:
                self.symbols_table['times'] = 500
            if config.mode == 0:
                self.compute_diff(symbol)

        def process_amplitude_message(amplitude):
            '''
            处理现货tick数据
            '''

            if amplitude < -0.08 and  time.time() - self.NN_batch > 3:
                self.NN_batch = time.time()
                if config.mode == 1:
                    self.catch_knife()
            #else:
                #print("no")

        def process_future_message(node):
            '''
            处理合约tick数据
            '''

            symbol = node['symbol'][:-4].lower()
            self.symbols_table[symbol]["future"] = node
            self.symbols_table[symbol]['future']['ws_delay'] = abs(self.symbols_table[symbol]['future']['time_receive']- self.symbols_table[symbol]['future']['time'] - self.utils.time_sync_future)
            self.symbols_table[symbol]["update_time_f"] = node['time']
            self.symbols_table[symbol]['update_time'] = node['time']
            self.symbols_table['times'] += 1
            #print(self.symbols_table)
            if self.symbols_table['times'] == 100000:
                self.symbols_table['times'] = 500
            if config.mode == 0:
                self.compute_diff(symbol)


        hb_spot = HBSpot_Websocket(host="wss://api.huobi.pro/ws", ping_interval=20)
        hb_spot.set_callback(process_spot_message)
        hb_spot.set_catchknife(process_amplitude_message)
        hb_spot.start()

        hb_future = HBCoinFuture_Websocket(host="wss://api.hbdm.com/swap-ws", ping_interval=20)
        hb_future.set_callback(process_future_message)
        hb_future.start()

    def to_exit(self):
        if self.count_error >= 3:
            self.dingding.send_text(msg=f"币本位程序运行异常，请检查！", at_mobiles= ['13609748076'])
            os._exit(0)


    def compute_diff(self, symbol):
        '''
        计算现货和合约的价差，如果超出阈值就打印或者发到钉钉
        '''

        if self.symbols_table["times"] > 20:

            self.symbols_table[symbol]["diff_time"] = round(self.symbols_table[symbol]["update_time_f"] - self.symbols_table[symbol]["update_time_s"],3)
            self.symbols_table[symbol]["price"] = (self.symbols_table[symbol]['future']['bid_price'] + self.symbols_table[symbol]['spot']['ask_price'])/2
            self.symbols_table[symbol]["quantity"] = min(self.symbols_table[symbol]['future']['bid_quantity'], self.symbols_table[symbol]['spot']['ask_quantity'])
            self.symbols_table[symbol]["negative_quantity"] = min(self.symbols_table[symbol]['future']['ask_quantity'],
                                                         self.symbols_table[symbol]['spot']['bid_quantity'])
            self.symbols_table[symbol]['negative_price'] = (self.symbols_table[symbol]['future']['ask_price'] + self.symbols_table[symbol]['spot']['bid_price'])/2
            self.symbols_table[symbol]["open_premium"] = (self.symbols_table[symbol]['future']['bid_price'] - self.symbols_table[symbol]['spot']['ask_price'])/self.symbols_table[symbol]['spot']['ask_price']
            self.symbols_table[symbol]["close_premium"] =  (self.symbols_table[symbol]['spot']['bid_price'] - self.symbols_table[symbol]['future']['ask_price'])/self.symbols_table[symbol]['future']['ask_price']

            #print(symbol, self.symbols_table[symbol]['future']['ws_delay'], self.symbols_table[symbol]['spot']['ws_delay'])
            #if self.symbols_table[symbol]["open_premium"] > 0.001:
            #print(symbol, self.symbols_table[symbol]["open_premium"])
            #print(is_settlement(self.symbols_table[symbol]['update_time']))
            #print(type(self.symbols_table[symbol]["open_premium"]), type(config.open_premium[symbol]))
            if self.symbols_table[symbol]["open_premium"] >= config.open_premium[symbol] and abs(self.symbols_table[symbol]["update_time_s"]-self.symbols_table[symbol]["update_time_f"])< 0.6 :
                #print("1")
                if self.symbols_table[symbol]['spot']['ws_delay'] < 0.12 and self.symbols_table[symbol]['future']['ws_delay'] < 0.12 and self.utils.is_settlement(self.symbols_table[symbol]['update_time'], symbol):
                    amt = self.symbols_table[symbol]['quantity'] * self.symbols_table[symbol]['price']
                    #print(amt)
                    if amt > 600 and self.utils.funding_fee[symbol] > -0.0010:
                        #print("3")
                        if self.utils.spot_usdt > 600 and time.time()-self.NN > 1:
                            #print("4")
                            self.NN = time.time()
                            #print(self.symbols_table[symbol], time.time())
                            #print(self.utils.spot_usdt)
                            with self.lock:
                                open_signal_first = self.utils.huobi_http_spot.place_order(self.utils.spot_id, symbol + 'usdt',
                                                                       'buy-market', 200)
                                if open_signal_first == 0:
                                    self.count_error += 1
                                    self.to_exit()
                            #print("make order", time.time())
                            self.utils.spot_usdt -= 200
                            print("After purchasing, amount of USDT is", self.utils.spot_usdt)
                            amt = math.floor(
                                    float(self.utils.get_spot_balance(symbol)) * 10 ** self.utils.spot_step_size[symbol][
                                        'amount_precision']) / 10 ** self.utils.spot_step_size[symbol]['amount_precision']

                            with self.lock:
                                open_transfer_signal = self.utils.huobi_http_spot.spot_contract_transfer('spot', 'swap', symbol, amt)
                                if open_transfer_signal == 0:
                                    self.count_error += 1
                                    self.to_exit()
                                index_price = self.utils.huobi_http_future.get_kline(symbol + '-usd', '5min', 1)[0]['close']  # self.symbols_table[symbol]['price']
                                # print("get index price:", index_price)

                            contract_amt = amt * index_price / 10
                            #print("calculating amount of contract:", contract_amt)
                            #if contract_amt - 1 > 1:
                            if contract_amt > 1:

                                with self.lock:
                                    open_signal_second = self.utils.huobi_http_future.place_order(contract_code=symbol + '-usd',
                                                                             #amount=math.floor(contract_amt - 1),
                                                                             amount=math.floor(contract_amt),
                                                                             direction='sell', offset='open',
                                                                             lever_rate=5, type='optimal_5')
                                    if open_signal_second == 0:
                                        self.count_error += 1
                                        self.to_exit()
                                print("Crate order", time.time())
                                self.dingding.send_text(msg=f"开仓啦！ 币种：{symbol}, amt: {math.floor(contract_amt-1)}", is_at_all=False)
                                print(" ")





            #if self.symbols_table[symbol]['close_premium'] > -0.005:
            #print(symbol, self.symbols_table[symbol]['close_premium'])
            #"""
            if self.symbols_table[symbol]["close_premium"] >= config.close_premium[symbol] and abs(self.symbols_table[symbol]["update_time_s"]-self.symbols_table[symbol]["update_time_f"]) <  0.55:
                if self.symbols_table[symbol]['spot']['ws_delay'] < 0.12 and self.symbols_table[symbol]['future']['ws_delay'] < 0.12:
                    if self.symbols_table[symbol]['negative_quantity'] * self.symbols_table[symbol]['negative_price'] > 600  and self.utils.is_settlement(self.symbols_table[symbol]['update_time'], symbol):
                        if self.utils.future_position[symbol]['available'] >= 20 and time.time() - self.NN > 1:
                            self.NN = time.time()
                            close_amount = 20
                            if self.symbols_table[symbol]["close_premium"] > 0.02 and self.symbols_table[symbol]["close_premium"] <= 0.04:
                                close_amount = 30
                            elif self.symbols_table[symbol]["close_premium"] > 0.04 and self.symbols_table[symbol]["close_premium"] <= 0.06:
                                close_amount = 40
                            elif self.symbols_table[symbol]["close_premium"] > 0.06:
                                close_amount = 60
                            with self.lock:
                                close_order_signal_first = self.utils.huobi_http_future.place_order(contract_code=symbol + '-usd',
                                                                             amount=close_amount,
                                                                             direction='buy', offset='close',
                                                                             lever_rate=5, type='optimal_5')
                                curr_future_price_df = self.utils.huobi_http_future.get_kline(symbol.upper() + '-USD', period="1min",
                                                                           size=1)#[0]['close']
                            if close_order_signal_first == 0:
                                self.count_error += 1
                                self.to_exit()
                            if curr_future_price_df == 0:
                                self.count_error += 1
                                self.to_exit()
                                curr_future_price = 0
                            else:
                                curr_future_price = curr_future_price_df[0]['close']

                            if close_order_signal_first != 0 and curr_future_price != 0:
                                transfer_amt = math.floor((close_amount * 10 / curr_future_price) * 0.995 * 10 ** (
                                self.utils.spot_step_size[symbol]['amount_precision'])) / 10 ** (
                                               self.utils.spot_step_size[symbol]['amount_precision'])
                                with self.lock:
                                    transfer_signal = self.utils.huobi_http_spot.spot_contract_transfer('swap', 'spot', symbol, transfer_amt)

                                if transfer_signal != 0:
                                    price = round(self.symbols_table[symbol]['spot']['bid_price'] * 0.95,
                                                  self.utils.spot_step_size[symbol]['price_precision'])
                                    with self.lock:
                                        close_signal_second = self.utils.huobi_http_spot.place_order(account_id=self.utils.spot_id,
                                                                               symbol=symbol + 'usdt',
                                                                               type='sell-limit',
                                                                               amount=transfer_amt, price=price)
                                        if close_signal_second == 0:
                                            self.count_error += 1
                                            self.to_exit()
                                        self.utils.future_position[symbol]['available'] -= close_amount
                                        self.utils.spot_usdt += close_amount * 10
                                        self.dingding.send_text(
                                            msg=f"平仓啦！ 币种：{symbol}, amt: {close_amount}",
                                            is_at_all=False)
                                        print(" ")
                                else:
                                    self.count_error += 1
                                    self.to_exit()
            #"""

    def catch_knife(self):
        contract_codes = []
        print(self.utils.future_position)
        for symbol in config.symbols:
            if self.utils.future_position[symbol]['available'] >= 150:
                contract_codes.append(symbol+'-usd')
        if len(contract_codes)  != 0:
            order_signal = self.utils.huobi_http_future.place_batchorder(contract_codes = contract_codes, volume=100, direction='buy',
                                           offset='close', lever_rate=5, type='optimal_20')
            if order_signal == 0:
                self.dingding.send_text(msg = f"批量下单failed！ 币种：{contract_codes}, amt: {100}", at_mobiles= ['13609748076'])
                #print("合约平仓异常！ 币种：{contract_codes}, amt: {100}")
            elif order_signal == 1:
                self.dingding.send_text(msg=f"批量下单successful！ 币种：{contract_codes}, amt: {100}",is_at_all=False)
                #print(f"平仓啦！ 币种：{contract_codes}, amt: {4}")
                for code in contract_codes:
                    self.utils.future_position[code[:-4]]['available'] -=100
        else:
            self.dingding.send_text(msg=f"没有可以平仓的合约了，请登录确认。", at_mobiles= ['13609748076'])




if __name__ == '__main__':
    data_huobi = DataHuobi()
    data_huobi.init_monitor()
