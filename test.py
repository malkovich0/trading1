import os
import sys
import time
import json
from datetime import datetime
import asyncio
import logging
import traceback

import pyupbit
import websockets
import multiprocessing as mp
from dateutil.relativedelta import relativedelta
import pickle
import pandas as pd
import numpy as np

from module import upbit

# -----------------------------------------------------------------------------
# - Name : upbit_ws_client
# - Desc : 업비트 웹소켓
# -----------------------------------------------------------------------------
async def upbit_ws_client(q,target):
    try:
        while True:
            if ('list_coins' in target.keys()) and (len(target) >= 4):
                if len(set(target['list_coins']) - set(target.keys()[3:])) == 0:
                    break
            time.sleep(1)

        subscribe_items = target['list_coins']
        print(f'websocket 조회종목 : {subscribe_items}')

        from pyupbit import WebSocketManager
        if __name__ == "__main__":
            wm = WebSocketManager('ticker', subscribe_items)

            while True:
                if subscribe_items != target['list_coins']:
                    await upbit_ws_client(q, target)
                data = wm.get()
                q.put(data)

    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception as e:
        logging.error('Exception Raised! upbit_ws_client')
        logging.error(e)
        logging.error('Connect Again!')

        # 웹소켓 다시 시작
        await upbit_ws_client(q,target)


# -----------------------------------------------------------------------------
# - Name : main
# - Desc : 메인
# -----------------------------------------------------------------------------
async def main_websocket(q,target):
    try:
        # 웹소켓 시작
        await upbit_ws_client(q,target)

    except Exception as e:
        logging.error('Exception Raised! main_websocket')
        logging.error(e)


def producer1(q,target):

    asyncio.run(main_websocket(q,target))

def producer2(target, qlog):
    logger = upbit.Log().config_queue_log(qlog, 'mp')
    asyncio.run(main_target(target, logger))


async def main_target(target, logger):
    try:
        making_target = wait_trading_target(target, logger)
        making_trading_variables = wait_trading_variables(target)
        await asyncio.gather(making_target, making_trading_variables)
    except Exception as e:
        logging.error('Exception Raised! main_target')
        logging.error(e)


async def wait_trading_target(target, logger):
    while True:
        time1 = datetime.now()
        # time2 = (time1 + relativedelta(days=0)).replace(hour=10,minute=15,second=0)
        # time2 = (time1 + relativedelta(days=0)).replace(hour=12,minute=39,second=0)
        time2 = time1 + relativedelta(seconds=10)
        await asyncio.sleep((time2 - time1).total_seconds())
        # print('Define Trading Target')
        # print('wait_target', )
        if len(target) == 0:
            await asyncio.sleep(1)
            continue
        else:
            for code_coin in target['list_coins']:
                target[code_coin] = define_trading_target(code_coin,target['value_per_trade'])
                await asyncio.sleep(0.1)
        for coin_del in list(set(target.keys()[3:]) - set(target['list_coins'])):
            del (target[coin_del])
        logger.log(20, f'Define Trading Target\n{target}')

        # print('Define Trading Target\n', target)

async def wait_trading_variables(target):
    while True:
        with open('./info/trade_info.pickle', 'rb') as handle:
            trade_info = pickle.load(handle)
        target['STATUS'] = trade_info['STATUS']
        target['list_coins'] = trade_info['list_coin_code']
        target['value_per_trade'] = trade_info['value_per_trade']
        # print('wait_var\n',target )
        logging.info(target)
        await asyncio.sleep(30)

def define_trading_target(code_coin, value):
    df_candle = pyupbit.get_ohlcv(code_coin, count=10)
    # range 계산
    price_range = np.array(df_candle['high'] - df_candle['low'])[-2]

    # k_value 정의
    k_value = np.maximum(0.5, np.abs(df_candle['open'] - df_candle['close']) / (df_candle['high'] - df_candle['low']))[-2]
    target_price = df_candle.close.iloc[-2] + k_value * price_range
    value_order = value

    # 종목, 날짜, 목표가, 주문총액, 주문종류, 주문상태, 매수주문가, 스탑로스가
    trading_target_temp = [code_coin, datetime.today().date(), target_price, value_order, 'buy', 0, np.nan, np.nan]
    return trading_target_temp


async def run_trading(real, target, qlog):
    logger = upbit.Log().config_queue_log(qlog, 'mp')
    target_copy = target.copy()
    while True:
        if not 'list_coins' in target.keys():
            continue
        if len(target) >= 4:
            if not len(set(target['list_coins']) - set(target.keys()[3:])) == 0:
                continue
        # if not 'list_coins' in target.keys():
        #     continue
        if not 'list_coins' in target_copy.keys():
            target_copy = target.copy()
        if not target['list_coins'] == target_copy['list_coins']:
            print(f'target 변경\n{target}\n{target_copy}')
            logger.log(20, f'target 변경\n{target}')
        target_copy = target.copy()

        # if not target == None:
        #     await asyncio.sleep(5)
        #     print('run\n',target)
            # print('길이 : ',real.qsize())
        if not target['STATUS']:
            print('--------------\n종료\n--------------')
            sys.exit()
        data = real.get()
        print(data)
        if data['ty'] == 'trade':
            if data['cd'] in target.keys():
                target_coin = target[data['cd']]
                print(target_coin)
#                if target_coin[5]:
#                    continue
#                if target_coin[2] <= data['tp']:
#                    remaining_asset = upbit.get_krwbal()
#                    order_value = target_coin[3]
#                    if order_value > remaining_asset['available_krw']:
#                        logger.log(20,f'잔고부족으로 주문 미실행\n{target_coin}')
#                        upbit.send_line_message(f'잔고부족으로 주문 미실행\n{target_coin}')
#                        continue
                    # rtn_buying_tg = upbit.buycoin_tg(target_coin[0], target_coin[3], target_coin[2])
#                    logger.log(20,f'주문실행\n{target_coin}')
#                    upbit.send_line_message(f'주문실행\n{target_coin}')
#                    target_coin[5] = 1
                    # target에 주문실행됐다고 coin의 status에 yes로 표시.

        # logger.log(10,data)
        # logging.info(data)
        # trading_target 주기적으로 가져오기. / 파일이 없으면 주기적으로 반복.
        # print(trading_target)
        # print('run trade vol')
        # run_trade_vol_strategy(data)


# -----------------------------------------------------------------------------
# - Name : main
# - Desc : 메인
# -----------------------------------------------------------------------------
if __name__ == "__main__":

    # noinspection PyBroadException
    try:

        # ---------------------------------------------------------------------
        # Logic Start!
        # ---------------------------------------------------------------------
        # 웹소켓 시작
        real = mp.Queue()
        manager = mp.Manager()

        qlog = mp.Queue()
        listener = upbit.Log()
        listener.listener_start('test','listener',qlog)

        target = manager.dict()
        p1 = mp.Process(name="Price_Receiver", target=producer1, args=(real,target,), daemon=True)
        p1.start()
        p2 = mp.Process(name="Target_Receiver", target=producer2, args=(target,qlog), daemon=True)
        p2.start()

        asyncio.run(run_trading(real, target, qlog))
        # run_trading(real, target)

    except KeyboardInterrupt:
        logging.error("KeyboardInterrupt Exception 발생!")
        logging.error(traceback.format_exc())
        sys.exit(-100)

    except Exception:
        logging.error("Exception 발생!")
        logging.error(traceback.format_exc())
        sys.exit(-200)