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
import pandas_ta as ta

# -----------------------------------------------------------------------------
# - Name : main_websocket
# - Desc : 실시간 자료 수집함수
# -----------------------------------------------------------------------------
def producer1(q,target, target_sell, qlog):
    logger = upbit.Log().config_queue_log(qlog, 'websocket')
    asyncio.run(main_websocket(q,target, target_sell, logger))

async def main_websocket(q,target,target_sell,logger):
    try:
        # 웹소켓 시작
        await upbit_ws_client(q,target,target_sell,logger)

    except Exception as e:
        logger.log(40,'Exception Raised! main_websocket')
        logger.log(40,e)

# -----------------------------------------------------------------------------
# - Name : upbit_ws_client
# - Desc : 업비트 웹소켓
# -----------------------------------------------------------------------------
async def upbit_ws_client(q,target,target_sell,logger):
    try:
        logger.log(20, 'target 대기 중......')
        upbit.send_line_message('target 대기 중......')
        while True:
            if ('list_coins' in target.keys()) and (len(target) >= 4):
                if len(set(target['list_coins']) - set(target.keys()[3:])) == 0:
                    break
        subscribe_items = target['list_coins'] + list(target_sell.keys())
        subscribe_items = list(dict.fromkeys(subscribe_items)) # 순서 유지한채 중복 제거
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
                    logger.log(20, f'list_coins 변경으로 websocket 재실행')
                    await upbit_ws_client(q, target, target_sell, logger)
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
        await upbit_ws_client(q,target, target_sell, logger)

# -----------------------------------------------------------------------------
# - Name : main_target
# - Desc : 거래대상 생성 함수
# -----------------------------------------------------------------------------
def producer2(target, target_sell, qlog):
    logger = upbit.Log().config_queue_log(qlog, 'target')
    asyncio.run(main_target(target, target_sell, logger))

async def main_target(target, target_sell, logger):
    try:
        making_trading_variables = wait_trading_variables(target, logger)
        making_target = wait_trading_target(target, logger)
        making_target_sell = wait_trading_target_sell(target_sell, logger)
        await asyncio.gather(making_trading_variables, making_target, making_target_sell)
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

async def wait_trading_target_sell(target_sell, logger):
# # 특정 시간이면 전일 보유 종목 전부 매도
    upbit_api = pyupbit.Upbit(upbit.access_key_pc, upbit.secret_key_pc)
    while True:
        time1_sell = datetime.now()
        if int(time1_sell.strftime("%H%M%S")) < 100000:
            time2_sell = (time1_sell + relativedelta(days=0)).replace(hour=10, minute=0, second=0)
        else:
            time2_sell = (time1_sell + relativedelta(days=1)).replace(hour=10,minute=0,second=0)
        logger.log(20, f'*****target_sell 대기***** \n대기완료시간 : {time2_sell}\n대기시간[분] : {(time2_sell - time1_sell).total_seconds()/60}')
        await asyncio.sleep((time2_sell - time1_sell).total_seconds())
        balances = upbit_api.get_balances()
        logger.log(20, f'*****매도목적 잔고조회결과*****\n{balances}')
        for coin in balances:
            if coin['currency'] in ['KRW','CPT']:
                continue
            else:
                coin_name = f'{coin["unit_currency"]}-{coin["currency"]}'
                target_sell[coin_name] = coin['balance']
                logger.log(20, f'*****매도 저장*****\n{coin_name}, {target_sell[coin_name]}')
        logger.log(20,f'*****매도 대상***** : {target_sell}')
        upbit.send_line_message(f'매도 대상 : {target_sell}')

async def wait_trading_target(target, logger):
    while True:
        time1 = datetime.now()
        if int(time1.strftime("%H%M%S")) <= 91000:
        # if int(time1.strftime("%H%M%S")) <= 124000:
            file_target = f'./target/target_{(time1 - relativedelta(days=1)).strftime("%y%m%d")}.pickle'
        else:
            file_target = f'./target/target_{time1.strftime("%y%m%d")}.pickle'
        logger.log(20,f'Target 저장파일명 : {file_target}')
        # code가 중간에 종료되어 재실행된 경우 저장된 target그대로 가져오기.
        if int(time1.strftime("%H%M%S")) <= 91000:
            time2 = (time1 + relativedelta(days=0)).replace(hour=9, minute=10, second=0)
        else:
            time2 = (time1 + relativedelta(days=1)).replace(hour=9,minute=10,second=0)
        # if int(time1.strftime("%H%M%S")) <= 124000:
        #     time2 = (time1 + relativedelta(days=0)).replace(hour=12, minute=40, second=0)
        # else:
        #     time2 = (time1 + relativedelta(days=1)).replace(hour=12, minute=40, second=0)
        logger.log(20,f'대기완료시간 : {time2}')
        if os.path.isfile(file_target):
            with open(file_target, 'rb') as handle:
                target = pickle.load(handle)
                # 과거에 저장했던 target을 불러온 경우 이미 주문이 실행되었을 수 있기때문에, 주문 내역을 조회하여 주문한 경우에는 빼줘야함.
                logger.log(20, f'저장된 target 조회\n{target}')
                upbit.send_line_message(f'저장된 target 조회\n{target}')
                await asyncio.sleep((time2 - time1).total_seconds())
        else:
            logger.log(20, f'다음 target 설정 시간 : {time2}\n대기시간[분] : {(time2 - time1).total_seconds()/60}')
            upbit.send_line_message(f'다음 target 설정 시간 : {time2}')
            await asyncio.sleep((time2 - time1).total_seconds())
            if len(target) == 0:
                logger.log(20, f'target을 아직 불러오지 못해서 반복 실행.')
                await asyncio.sleep(1)
                continue
            else:
                logger.log(20, f'target 저장')
                for code_coin in target['list_coins']:
                    target[code_coin] = define_trading_target(code_coin,target['value_per_trade'])
                    time.sleep(0.1)
            for coin_del in list(set(target.keys()[3:]) - set(target['list_coins'])):
                del (target[coin_del])
                logger.log(20, f'전일 target 중 금일 대상 제외종목 삭제 : {coin_del}')
            file_name = f'./target/target_{time2.strftime("%H%M%S")}.pickle'
            with open(file_name,'wb') as handle:
                pickle.dump(target, handle, protocol=pickle.HIGHEST_PROTOCOL)
                logger.log(20, f'다음 파일명으로 target 저장 : {file_name}')
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

    # filter 여부
    # filter 여부
    filter_value = 0
    noise_maximum = 0.4
    noise_ma = cal_ma((df_candle.high - df_candle.close) / (df_candle.high - df_candle.low), method='sma', length=5)
    volume_minimum = 100000000000
    volume_ma = cal_ma(df_candle.value, method='sma', length=5)
    price_last = df_candle.close[-2]
    price_ma = cal_ma(df_candle.close, method='sma', length=6)
    volume_last = df_candle.value[-2]

    if noise_ma[-2] < noise_maximum:
        filter_value = 1
    elif volume_ma[-2] > volume_minimum:
        filter_value = 1
    elif price_ma[-2] < price_last:
        filter_value = 1
    #    elif volume_ma[-2] < volume_last:
    #        return None
    else:
        pass
        # 종목, 날짜, 목표가, 주문총액, 주문종류, 주문상태, 매수주문가, 스탑로스가
    trading_target_temp = [code_coin, datetime.today().strftime("%y%m%d"), target_price, value_order, 'buy', filter_value, np.nan, np.nan]
    return trading_target_temp

def cal_ma(data, method = 'sma', length=5):
    if method == 'sma':
        ma_result = ta.sma(data, length=length, talib=False)
    elif method == 'ema':
        ma_result = ta.ema(data, length=length, talib=False)
    elif method == 'wma':
        ma_result = ta.wma(data, length=length, talib=False)
    else:
        raise ValueError('Check ma method')
    return ma_result

# -----------------------------------------------------------------------------
# - Name : main_log
# - Desc : 거래 실행 함수
# -----------------------------------------------------------------------------
# order관리
def producer3(qlog):
    logger = upbit.Log().config_queue_log(qlog, 'order')
    asyncio.run(main_order(logger))

async def main_order(logger):
    try:
        upbit_api = pyupbit.Upbit(upbit.access_key_pc, upbit.secret_key_pc)

        dict_order = {'start_time':datetime.now()}
        rtn_wait_order = upbit_api.get_order_list(state='wait')
        rtn_done_order = upbit_api.get_order_list(state='done')
        rtn_order = rtn_wait_order + rtn_done_order
        if len(rtn_order) == 0:
            pass
        else:
            for order in rtn_order:
                dict_order[order['uuid']] = [order['market'], order['side'], order['price'], order['volume'], order['remaining_volume']]
        mng_reset_order = reset_daily_order(upbit_api, logger)
        mng_call_order = manage_call_order(upbit_api, dict_order, logger)
        await asyncio.gather(mng_reset_order, mng_call_order)
    except Exception as e:
        logger.log(40, 'Exception Raised! main_order')
        logger.log(40, e)
        sys.exit()

async def reset_daily_order(upbit_api, logger):

    target_hour = 9
    while True:
        time_now = datetime.now()
        # 특정 시간이면 미체결 매수 주문 전부 취소
        if (time_now.replace(hour=target_hour, minute=0, second=0) < time_now) & (time_now.replace(hour=target_hour, minute=10, second=0) > time_now):
            rtn_wait_order = upbit_api.get_order_list(state='wait') # get_order_list 함수 만들어서 추가하기.
            if len(rtn_wait_order) != 0:
                for each_order in rtn_wait_order:
                    if each_order['side'] == 'ask':  # 매도주문은 제외 (ask : 매도, bid : 매수)
                        continue
                    rtn_order_cancel = upbit_api.cancel_order(each_order['uuid'])
                    logger.log(20, rtn_order_cancel)
                    upbit.send_line_message(f"미체결 주문 취소 : {rtn_order_cancel['market']}")
            else:
                logger.log(20, '미체결 주문 없음')
                upbit.send_line_message('미체결 주문 없음', time_now)
            await asyncio.sleep(600)
        # # 특정 시간이면 전일 보유 종목 전부 매도
        # elif (time_now.replace(hour=10, minute=0, second=0) < time_now) & (time_now.replace(hour=10, minute=10, second=0) > time_now):
        #     balances = upbit_api.get_balances()
        #     for coin in balances:
        #         if coin['currency'] in ['KRW','CPT']:
        #             continue
        #         else:
        #
        else:
            await asyncio.sleep(60)

def strtime_to_datetime(strtime):
    date_temp = strtime.split('T')[0]
    time_temp = strtime.split('T')[1]
    return datetime(date_temp[:4], date_temp[5:7], date_temp[8:10], time_temp[0:2], time_temp[3:5], time_temp[6:8])

# 초단위로 체결여부 받아서, 신규주문 logging. 체결주몬 logging. 체결시점 기준으로 잔고없으면 취소.
# 주문조회함수 새로 구성.
# 주문접수시간만 나와서 체결이 된 종목을 time_now의 방식으로 기록할 수 없음.
# base dict를 만들어서 uuid를 기준으로 주문조회하여 base dict와 다른 경우가 있으면 저장하면서 logging. 다시 시작한 경우면 base dict를 가져오기. 새로운 uuid가 생기면 신규주문이므로 저장 및 logging.
# base_dict는 직전 계좌 상황을 표현. dict key : uuid, value : ['market','side','price','volume','remaining_vloume'] 종목,매수매도,가격,거래량,잔여량.
# wait, done은 base_dict의 uuid조회하여 변경되었으면 반영, base_dict에 없으면 추가.

async def manage_call_order(upbit_api, dict_order, logger):

    while True:
        # dict_order 관리, 주문이 완료되고, 며칠 지난 경우에는 dict에서 삭제

        # 주문 금액이 잔여 금액보다 큰 경우 취소.
        rtns_wait_order = upbit_api.get_order_list('wait')  #함수 생성 필요
        remaining_asset = upbit_api.get_balance('KRW')
        for rtn_wait_order in rtns_wait_order:
            order_value = float(rtn_wait_order['remaining_volume'])*float(rtn_wait_order['price'])

            if order_value > remaining_asset:
                rtn_order_cancel = upbit_api.cancel_order(rtn_wait_order['uuid'])
                upbit.send_line_message(f"잔고부족으로 미체결물량 취소 : {rtn_wait_order['market']}")
                logger.log(20, rtn_order_cancel)

        # dict_order 대비 변경사항이 있으면 저장.
        rtn_wait_order_temp = upbit_api.get_order_list('wait')
        rtn_done_order_temp = upbit_api.get_order_list('done')
        rtn_order_temp = rtn_wait_order_temp + rtn_done_order_temp
        for order_temp in rtn_order_temp:
            if order_temp['uuid'] in dict_order.keys():
                if float(order_temp['remaining_volume']) != float(dict_order[order_temp['uuid']][4]):
                    print(order_temp['remaining_volume'])
                    print(dict_order[order_temp['uuid']][4])
                    dict_order[order_temp['uuid']][4] = order_temp['remaining_volume']
                    logger.log(20, f"주문 체결 \n {order_temp}")
                    upbit.send_line_message(f"체결내용 저장, {order_temp['market']}, {order_temp['volume']}, {order_temp['remaining_volume']}")
            else:
                dict_order[order_temp['uuid']] = [order_temp['market'], order_temp['side'], order_temp['price'], order_temp['volume'], order_temp['remaining_volume']]
                logger.log(20, f"신규 주문 \n {order_temp}")
                upbit.send_line_message(f"신규 주문, {order_temp['market']}, {order_temp['volume']}, {order_temp['remaining_volume']}")
        await asyncio.sleep(1)





# -----------------------------------------------------------------------------
# - Name : run_trading
# - Desc : 거래 실행 함수
# -----------------------------------------------------------------------------
def run_trading(real, target, target_sell, qlog):
    # time.sleep(5)
    logger = upbit.Log().config_queue_log(qlog, 'trading')
    logger.log(20, "거래실행")
    upbit.send_line_message("거래실행")
    target_copy = target.copy()
    upbit_api = pyupbit.Upbit(upbit.access_key_pc, upbit.secret_key_pc)
    while True:
        if not (p1.is_alive())&(p2.is_alive())&(p3.is_alive()):
            listener.listener_end(qlog)
            sys.exit()
        if not 'list_coins' in target.keys():
            time.sleep(0.1)
            continue
        if not target['STATUS']:
            print('--------------\n종료\n--------------')
            logger.log(20, '--------------\n종료\n--------------')
            sys.exit()
        if len(target) >= 4:
            if not len(set(target['list_coins']) - set(target.keys()[3:])) == 0:
                time.sleep(0.1)
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
                if (target_coin[2] <= data['tp'])&(target_coin[2] >= data['tp']*0.99):  # 이미 가격이 오른 경우에는 매수안함.
                    remaining_asset = upbit_api.get_balance('KRW')
                    order_value = target_coin[3]
                    if order_value > remaining_asset:
                        logger.log(20,f'잔고부족으로 주문 미실행\n{target_coin}')
                        upbit.send_line_message(f'잔고부족으로 주문 미실행\n{target_coin}')
                        continue
                    # rtn_order_buy = upbit_api.buy_limit_order(ticker=target_coin[0], volume=target_coin[3], price=target_coin[2])
                    # logger.log(20,f'매수주문실행\n{rtn_order_buy}')
                    # upbit.send_line_message(f'매수주문실행\n{rtn_order_buy}')
                    logger.log(20,f'매수주문실행\n{target_coin}')
                    upbit.send_line_message(f'매수주문실행\n{target_coin}')
                    # target에 주문실행됐다고 coin의 status에 yes로 표시.
                    target_coin[5] = 1
                    target[data['cd']] = target_coin
                    # target file로 저장.
                    time1 = datetime.now()
                    if int(time1.strftime("%H%M%S")) < 91000:
                        file_target = f'./target/target_{(time1 - relativedelta(days=1)).strftime("%y%m%d")}.pickle'
                    else:
                        file_target = f'./target/target_{time1.strftime("%y%m%d")}.pickle'
                    with open(file_target, 'wb') as handle:
                        pickle.dump(target, handle, protocol=pickle.HIGHEST_PROTOCOL)
            if data['cd'] in target_sell.keys():
                coin_code = data['cd']
                price = data['tp']
                volume = target_sell[coin_code]
                # rtn_order_sell = upbit_api.sell_limit_order(ticker=coin_code, price=price, volume=volume)
                # logger.log(20, f'매도주문실행\n{rtn_order_sell}')
                # upbit.send_line_message(f'매도주문실행\n{rtn_order_sell}')
                logger.log(20, f'매도주문실행\n종목 : {coin_code}, 가격 : {price}, 거래량 : {volume}')
                upbit.send_line_message(f'매도주문실행\n종목 : {coin_code}, 가격 : {price}, 거래량 : {volume}')

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
        manager1 = mp.Manager()
        target = manager1.dict({'STATUS':1,'list_coins':[],'value_per_trade':0})
        manager2 = mp.Manager()
        target_sell = manager2.dict()

        p1 = mp.Process(name="Price_Receiver", target=producer1, args=(real,target,target_sell,qlog), daemon=True)
        p1.start()
        logger.log(20, 'Websocket Process 실행')
        p2 = mp.Process(name="Target_Receiver", target=producer2, args=(target,target_sell,qlog), daemon=True)
        p2.start()
        logger.log(20, 'target Process 실행')
        p3 = mp.Process(name="Order_Manager", target=producer3, args=(qlog,), daemon=True)
        p3.start()
        logger.log(20, 'Order Process 실행')

        # asyncio.run(run_trading(real, target, qlog))
        run_trading(real, target, target_sell, qlog)
        logger.log(20, 'trading Process 실행')

        # time.sleep(3)
        # print('종료')
        # p1.kill()
        # p2.terminate()
        # p3.terminate()
        # print(p1.is_alive())

    except KeyboardInterrupt:
        logger.log(40,"KeyboardInterrupt Exception 발생!")
        logger.log(40, traceback.format_exc())
        p1.terminate()
        p2.terminate()
        p3.terminate()
        sys.exit(1)

    except Exception:
        logger.log(40, "Exception 발생!")
        logger.log(40, traceback.format_exc())
        p1.terminate()
        p2.terminate()
        p3.terminate()
        sys.exit(2)