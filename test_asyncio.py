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
# - Name : main_websocket
# - Desc : 실시간 자료 수집함수
# -----------------------------------------------------------------------------
def producer1(q,target, qlog):
    logger = upbit.Log().config_queue_log(qlog, 'websocket')
    asyncio.run(main_websocket(q,target,logger))

async def main_websocket(q,target,logger):
    try:
        # 웹소켓 시작
        await upbit_ws_client(q,target,logger)

    except Exception as e:
        logger.log(40,'Exception Raised! main_websocket')
        logger.log(40,e)

# -----------------------------------------------------------------------------
# - Name : upbit_ws_client
# - Desc : 업비트 웹소켓
# -----------------------------------------------------------------------------
async def upbit_ws_client(q,target,logger):
    try:
        logger.log(20, 'target 대기 준......')
        upbit.send_line_message('target 대기 중......')
        while True:
            if ('list_coins' in target.keys()) and (len(target) >= 4):
                if len(set(target['list_coins']) - set(target.keys()[3:])) == 0:
                    break
        subscribe_items = target['list_coins']
        logger.log(20,f'websocket 조회종목({len(subscribe_items)}) : {subscribe_items}')
        upbit.send_line_message(f'websocket 조회종목({len(subscribe_items)}) : {subscribe_items}')

        # 구독 데이터 조립
        subscribe_fmt = [
            {"ticket": "test-websocket"},
            {
                "type": "trade",
                "codes": subscribe_items,
                "isOnlyRealtime": True
            },
            {"format": "SIMPLE"}
        ]

        subscribe_data = json.dumps(subscribe_fmt)

        async with websockets.connect(upbit.ws_url) as websocket:
            await websocket.send(subscribe_data)
            while True:
                if subscribe_items != target['list_coins']:
                    await upbit_ws_client(q, target)
                data = await websocket.recv()
                data = json.loads(data)
                q.put(data)

    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception as e:
        logger.log(40,'Exception Raised! upbit_ws_client')
        logger.log(40,e)
        logger.log(40,'Connect Again!')

        # 웹소켓 다시 시작
        await upbit_ws_client(q,target)

# -----------------------------------------------------------------------------
# - Name : main_target
# - Desc : 거래대상 생성 함수
# -----------------------------------------------------------------------------
def producer2(target, qlog):
    logger = upbit.Log().config_queue_log(qlog, 'target')
    asyncio.run(main_target(target, logger))

async def main_target(target, logger):
    try:
        making_trading_variables = wait_trading_variables(target, logger)
        making_target = wait_trading_target(target, logger)
        await asyncio.gather(making_trading_variables, making_target)
    except Exception as e:
        logger.log(40, 'Exception Raised! main_target')
        logger.log(40, e)

async def wait_trading_variables(target, logger):
    while True:
        with open('./info/trade_info.pickle', 'rb') as handle:
            trade_info = pickle.load(handle)
        if target['STATUS'] != trade_info['STATUS']:
            target['STATUS'] = trade_info['STATUS']
            logger.log(20, f"STATUS 재정의 : {target['STATUS']} -> {trade_info['STATUS']}")
        if (len(set(target['list_coins'])-set(trade_info['list_coin_code']))!=0)or(len(set(trade_info['list_coin_code'])-set(target['list_coins']))!=0):
            target['list_coins'] = trade_info['list_coin_code']
            logger.log(20, f"list_coins 재정의 : {target['list_coins']} -> {trade_info['list_coin_code']}")
        if target['value_per_trade'] != trade_info['value_per_trade']:
            target['value_per_trade'] = trade_info['value_per_trade']
            logger.log(20, f"value_per_trade 재정의 : {target['value_per_trade']} -> {trade_info['value_per_trade']}")
        await asyncio.sleep(30)

async def wait_trading_target(target, logger):
    while True:
        time1 = datetime.now()
        time2 = (time1 + relativedelta(days=1)).replace(hour=9,minute=10,second=0)
        # time2 = (time1 + relativedelta(days=0)).replace(hour=12,minute=33,second=0)
        # time2 = time1 + relativedelta(seconds=300)
        logger.log(20, f'다음 target 설정 시간 : {time2}')
        upbit.send_line_message(f'다음 target 설정 시간 : {time2}')
        await asyncio.sleep((time2 - time1).total_seconds())
        if len(target) == 0:
            await asyncio.sleep(1)
            continue
        else:
            for code_coin in target['list_coins']:
                target[code_coin] = define_trading_target(code_coin,target['value_per_trade'])
                time.sleep(0.1)
        for coin_del in list(set(target.keys()[3:]) - set(target['list_coins'])):
            del (target[coin_del])
        logger.log(20, f'Trading Target 정의\n{target}')
        upbit.send_line_message(f'Trading Target 정의\n{target}')

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

# -----------------------------------------------------------------------------
# - Name : run_trading
# - Desc : 거래 실행 함수
# -----------------------------------------------------------------------------
def run_trading(real, target, qlog):
    # time.sleep(5)
    logger = upbit.Log().config_queue_log(qlog, 'trading')
    logger.log(20, "거래실행")
    upbit.send_line_message("거래실행")
    target_copy = target.copy()
    upbit_api = pyupbit.Upbit(upbit.access_key, upbit.secret_key)
    while True:
        if not 'list_coins' in target.keys():
            continue
        if not target['STATUS']:
            print('--------------\n종료\n--------------')
            logger.log(20, '--------------\n종료\n--------------')
            sys.exit()
        if len(target) >= 4:
            if not len(set(target['list_coins']) - set(target.keys()[3:])) == 0:
                continue
        if not 'list_coins' in target_copy.keys():
            target_copy = target.copy()
        if not target['list_coins'] == target_copy['list_coins']:
            logger.log(20, f'target 변경\n변경전 : {target}\n변경후 : {target_copy}')
        target_copy = target.copy()
        data = real.get()
        if data['ty'] == 'trade':
            if data['cd'] in target.keys():
                target_coin = target[data['cd']]
                if target_coin[5]:
                    continue
                if target_coin[2] <= data['tp']:
                    remaining_asset = upbit_api.get_balance('KRW')
                    order_value = target_coin[3]
                    if order_value > remaining_asset:
                        logger.log(20,f'잔고부족으로 주문 미실행\n{target_coin}')
                        upbit.send_line_message(f'잔고부족으로 주문 미실행\n{target_coin}')
                        continue
                    # rtn_buying_tg = upbit.buycoin_tg(target_coin[0], target_coin[3], target_coin[2])
                    logger.log(20,f'주문실행\n{target_coin}')
                    upbit.send_line_message(f'주문실행\n{target_coin}')
                    # target에 주문실행됐다고 coin의 status에 yes로 표시.
                    target_coin[5] = 1
                    target[data['cd']] = target_coin

# -----------------------------------------------------------------------------
# - Name : main
# - Desc : 메인
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    qlog = mp.Queue()
    listener = upbit.Log()
    listener.listener_start('test', 'listener', qlog)
    logger = upbit.Log().config_queue_log(qlog, 'main')

    try:
        # ---------------------------------------------------------------------
        # Logic Start!
        # ---------------------------------------------------------------------
        # 웹소켓 시작
        logger.log(20, 'Main 실행')
        upbit.send_line_message('Main 실행')
        real = mp.Queue()
        manager = mp.Manager()
        target = manager.dict({'STATUS':1,'list_coins':[],'value_per_trade':0})

        p1 = mp.Process(name="Price_Receiver", target=producer1, args=(real,target,qlog), daemon=True)
        p1.start()
        logger.log(20, 'Websocket Process 실행')
        p2 = mp.Process(name="Target_Receiver", target=producer2, args=(target,qlog), daemon=True)
        p2.start()
        logger.log(20, 'target Process 실행')

        # asyncio.run(run_trading(real, target, qlog))
        run_trading(real, target, qlog)
        logger.log(20, 'trading Process 실행')

    except KeyboardInterrupt:
        logger.log(40,"KeyboardInterrupt Exception 발생!")
        logger.log(40, traceback.format_exc())
        sys.exit(-100)

    except Exception:
        logger.log(40, "Exception 발생!")
        logger.log(40, traceback.format_exc())
        sys.exit(-200)