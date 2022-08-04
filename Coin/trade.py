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

from telegram.ext import Updater
from telegram.ext import CommandHandler

# -----------------------------------------------------------------------------
# - Name : main_websocket
# - Desc : 실시간 자료 수집함수
# -----------------------------------------------------------------------------
def producer1(qreal, target, qlog):
    logger = upbit.Log().config_queue_log(qlog, 'websocket')
    asyncio.run(main_websocket(qreal,target,logger))

async def main_websocket(qreal,target,logger):
    try:
        # print('websocket 실행')
        logger.log(20,'websocket 실행')
        upbit.send_telegram_message('websocket 실행')
        # target이 저장됐는지 확인
        # date가 오늘인지 확인. (확인 후 10초 waiting)
        while True:
            if int(target['date']) == int(datetime.now().strftime('%y%m%d')):
                # print('일자확인완료')
                logger.log(20, f'target일자 확인 {target["date"]}')
                await asyncio.sleep(10)
                if len(target) > 4:
                    break
            else:
                await asyncio.sleep(10)
            await asyncio.sleep(1)
        subscribe_items = target['list_coins']
        logger.log(20, f'websocket 조회종목 ({len(subscribe_items)}) : {subscribe_items}')
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
            # print('websocket 구독시작')
            logger.log(20,f'websocket 구독시작\n{subscribe_items}')
            upbit.send_telegram_message(f'websocket 구독시작\n{subscribe_items}')
            while True:
                if subscribe_items != target['list_coins']:
                    logger.log(20,f'websocket 재실행')
                    await main_websocket(qreal, target, logger)
                data = await websocket.recv()
                data = json.loads(data)
                qreal.put(data)

    except Exception as e:
        # print('websocket Error')
        logger.log(40, 'Exception Raised!')
        logger.log(40,e)

# -----------------------------------------------------------------------------
# - Name : main_target
# - Desc : 거래대상 생성 함수
# -----------------------------------------------------------------------------
def producer2(target, qlog):
    logger = upbit.Log().config_queue_log(qlog, 'target')
    asyncio.run(main_target(target, logger))

async def main_target(target, logger):
    try:
        # print('target 실행')
        logger.log(20,'target 실행')
        upbit.send_telegram_message('target 실행')
        making_trading_variables = wait_trading_variables(target, logger)
        making_target = wait_trading_target(target, logger)
        await asyncio.gather(making_trading_variables, making_target)
    except Exception as e:
        # print('target Error')
    #     print(e)
        logger.log(40, 'Exception Raised!')
        logger.log(40, e)

async def wait_trading_variables(target, logger):
    while True:
        with open('./info/trade_info.pickle', 'rb') as handle:
            trade_info = pickle.load(handle)
        if target['STATUS'] != trade_info['STATUS']:
            if trade_info['STATUS'] >= 10:
                logger.log(20, f"STATUS 재정의 : {target['STATUS']} -> {trade_info['STATUS']}")
                target['STATUS'] = trade_info['STATUS']
        if (len(set(target['list_coins'])-set(trade_info['list_coin_code']))!=0)or(len(set(trade_info['list_coin_code'])-set(target['list_coins']))!=0):
            logger.log(20, f"list_coins 재정의 : {target['list_coins']} -> {trade_info['list_coin_code']}")
            target['list_coins'] = trade_info['list_coin_code']
        if target['value_per_trade'] != trade_info['value_per_trade']:
            logger.log(20, f"value_per_trade 재정의 : {target['value_per_trade']} -> {trade_info['value_per_trade']}")
            target['value_per_trade'] = trade_info['value_per_trade']
        await asyncio.sleep(60)

async def wait_trading_target(target, logger):
    while True:
        time_start = datetime.now()
        if int(time_start.strftime("%H%M%S")) < 91000:
            time_target = (time_start - relativedelta(days=1)).replace(hour=9, minute=10, second=0)
            file_target = f'./target/target_{time_target.strftime("%y%m%d")}.pickle'
            time_next_target = time_start.replace(hour=9, minute=10, second=0)
            # 9시 전에 시작되었다면, 무조건 중간에 다시시작하는 경우. (target 미정의)
            if os.path.isfile(file_target):
                logger.log(20, f'저장된 target 가져오기\n{file_target}')
                upbit.send_telegram_message(f'저장된 target 가져오기\n{file_target}')
                with open(file_target, 'rb') as handle:
                    target_dict = pickle.load(handle)
                    for key_target in target_dict.keys():
                        target[key_target] = target_dict[key_target]
            else:
                logger.log(20, f'target date 변경 : {time_target.strftime("%y%m%d")}')
                target['date'] = int(time_target.strftime("%y%m%d"))
                logger.log(20, f'target 종목 설정 : {target["list_coins"]}')
                upbit.send_telegram_message(f'target 종목 설정 : {target["list_coins"]}')
                for code_coin in target['list_coins']:
                    target[code_coin] = define_trading_target(code_coin, target['value_per_trade'])
                    await asyncio.sleep(0.1)
                logger.log(20, f'target 저장\n{target}')
                with open(file_target, 'wb') as handle:
                    pickle.dump(dict(target), handle, protocol=pickle.HIGHEST_PROTOCOL)
            logger.log(20, f'다음 target 생성 시점까지 대기 : {time_next_target}')
            await asyncio.sleep((time_next_target - datetime.now()).total_seconds())
        elif (int(time_start.strftime("%H%M%S")) >= 91000) & (int(time_start.strftime("%H%M%S")) < 100000):
            logger.log(20, f'미체결 주문 취소')
            target['STATUS'] = 1
            time_target = time_start.replace(hour=9, minute=10, second=0)
            file_target = f'./target/target_{time_target.strftime("%y%m%d")}.pickle'
            time_sell = time_start.replace(hour=10, minute=0, second=0)
            if os.path.isfile(file_target):
                logger.log(20, f'저장된 target 가져오기\n{file_target}')
                upbit.send_telegram_message(f'저장된 target 가져오기\n{file_target}')
                with open(file_target, 'rb') as handle:
                    target_dict = pickle.load(handle)
                    for key_target in target_dict.keys():
                        target[key_target] = target_dict[key_target]
            else:
                logger.log(20, f'target date 변경 : {time_target.strftime("%y%m%d")}')
                target['date'] = int(time_target.strftime("%y%m%d"))
                logger.log(20, f'target 종목 설정 : {target["list_coins"]}')
                upbit.send_telegram_message(f'target 종목 설정 : {target["list_coins"]}')
                for code_coin in target['list_coins']:
                    target[code_coin] = define_trading_target(code_coin, target['value_per_trade'])
                    await asyncio.sleep(0.1)
                logger.log(20, f'target 저장\n{target}')
                with open(file_target, 'wb') as handle:
                    pickle.dump(dict(target), handle, protocol=pickle.HIGHEST_PROTOCOL)
            logger.log(20, f'보유 종목 매도 시점까지 대기 : {time_sell}')
            await asyncio.sleep((time_sell - datetime.now()).total_seconds())
            logger.log(20, f'보유종목 전량 매도')
            target['STATUS'] = 2

        # 정상작동 중 10시가 되어 여기 넘어오면 target이 이미 정의되어 있다.
        elif int(time_start.strftime("%H%M%S")) >= 100000:
            time_target = time_start.replace(hour=9, minute=10, second=0)
            file_target = f'./target/target_{time_target.strftime("%y%m%d")}.pickle'
            time_next_target = (time_start + relativedelta(days=1)).replace(hour=9, minute=10, second=0)
            # 프로그램 실행 중
            if len(target)>4:
                logger.log(20, f'다음 target 생성 시점까지 대기 : {time_next_target}')
                await asyncio.sleep((time_next_target - datetime.now()).total_seconds())
            # 프로그램 재실행
            else:
                if os.path.isfile(file_target):
                    logger.log(20, f'저장된 target 가져오기\n{file_target}')
                    upbit.send_telegram_message(f'저장된 target 가져오기\n{file_target}')
                    with open(file_target, 'rb') as handle:
                        target_dict = pickle.load(handle)
                        for key_target in target_dict.keys():
                            target[key_target] = target_dict[key_target]
                else:
                    logger.log(20, f'target date 변경 : {time_target.strftime("%y%m%d")}')
                    target['date'] = int(time_target.strftime("%y%m%d"))
                    logger.log(20, f'target 종목 설정 : {target["list_coins"]}')
                    upbit.send_telegram_message(f'target 종목 설정 : {target["list_coins"]}')
                    for code_coin in target['list_coins']:
                        target[code_coin] = define_trading_target(code_coin, target['value_per_trade'])
                        await asyncio.sleep(0.1)
                    logger.log(20, f'target 저장\n{target}')
                    with open(file_target, 'wb') as handle:
                        pickle.dump(dict(target), handle, protocol=pickle.HIGHEST_PROTOCOL)
                logger.log(20, f'다음 target 생성 시점까지 대기 : {time_next_target}')
                await asyncio.sleep((time_next_target - datetime.now()).total_seconds())
        else:
            logger.log(20, '일자 잘못 설정되어 취소')
            sys.exit()

def define_trading_target(code_coin, value):
    df_candle = pyupbit.get_ohlcv(code_coin, count=30)
    # range 계산
    price_range = np.array(df_candle['high'] - df_candle['low'])[-2]

    # k_value 정의
    k_value = np.maximum(0.5, np.abs(df_candle['open'] - df_candle['close']) / (df_candle['high'] - df_candle['low']))[-2]
    target_price = df_candle.close.iloc[-2] + k_value * price_range
    value_order = value

    # filter 여부
    filter_value = 0
    noise_maximum = 0.4
    noise_ma = cal_ma((df_candle.high - df_candle.close) / (df_candle.high - df_candle.low), method='sma', length=5)
    volume_minimum = 100000000000
    volume_ma = cal_ma(df_candle.value, method='sma', length=5)
    price_last = df_candle.close[-2]
    price_ma = cal_ma(df_candle.close, method='wma', length=8)
    volume_last = df_candle.value[-2]

    if filter_value:
        pass
    # elif noise_ma[-2] < noise_maximum:
    #     filter_value = 1
    # elif volume_ma[-2] < volume_minimum:
    #     filter_value = 1
    elif volume_last < volume_minimum:
        filter_value = 1
    elif price_ma[-2] > price_last:
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
# - Name : main_telegram
# - Desc : telegram 연결 관리 함수
# -----------------------------------------------------------------------------
def producer3(upbit_api, target):
    main_telegram(upbit_api, target)

def main_telegram(upbit_api, target):
    try:
        # step2.Updater(유저의 입력을 계속 모니터링하는 역할), Dispatcher
        updater = Updater(token=upbit.telegram_token, use_context=True)
        dispatcher = updater.dispatcher

        # step3./start 명령어가 입력되었을 때의 함수 정의
        # 잔고조회
        def call_balance(update, context):
            rtn_balances = upbit_api.get_balances()
            if len(rtn_balances) == 0:
                msg = '보유 잔고 없음'
            else:
                msg = rtn_balances
            context.bot.send_message(chat_id=update.effective_chat.id, text=f'<보유 잔고 조회>\n{msg}')

        # 미체결조회
        def call_wait_order(update, context):
            rtn_wait_order = upbit_api.get_order_list(state='wait')
            if len(rtn_wait_order) == 0:
                msg = '미체결 종목 없음'
            else:
                msg = rtn_wait_order
            context.bot.send_message(chat_id=update.effective_chat.id, text=f'<미체결 종목 조회>\n{msg}')

        def call_target(update, context):
            msg = target
            context.bot.send_message(chat_id=update.effective_chat.id, text=f'<target 조회>\n{msg}')

        def call_log(update, context):
            f = open('./logs/log', 'r', encoding='utf-8')
            context.bot.send_message(chat_id=update.effective_chat.id, text=f'<log 조회>')
            for msg in f.readlines():
                context.bot.send_message(chat_id=update.effective_chat.id, text=f'{msg}')

        def call_price(update, context):
            if context.args[0] == 'target':
                for coin_code in target['list_coins']:
                    rtn_price = pyupbit.get_ohlcv(coin_code, count=1)
                    msg = []
                    for col in list(rtn_price.columns):
                        msg.append(f'{col} : {rtn_price[col][0]}')
                    context.bot.send_message(chat_id=update.effective_chat.id,
                                             text=f'<{coin_code} 가격 조회({rtn_price.index[0].strftime("%y%m%d")})>\n{msg}')
            else:
                coin_code = f'KRW-{context.args[0].upper()}'
                rtn_price = pyupbit.get_ohlcv(coin_code, count=1)
                msg = []
                for col in list(rtn_price.columns):
                    msg.append(f'{col} : {rtn_price[col][0]}')
                context.bot.send_message(chat_id=update.effective_chat.id,
                                         text=f'<{coin_code} 가격 조회({rtn_price.index[0].strftime("%y%m%d")})>\n{msg}')

        # def call_stop(update, context):
        #     sys.exit()

        # 전량매도
        # def call_sellall_order(update, context):

        # 미체결취소
        # def call_cancel_wait_order(update, context):

        # step4.위에서 정의한 함수를 실행할 CommandHandler 정의
        balance_handler = CommandHandler('balance', call_balance)
        wait_handler = CommandHandler('wait', call_wait_order)
        target_handler = CommandHandler('target', call_target)
        log_handler = CommandHandler('log', call_log)
        price_handler = CommandHandler('price', call_price)
        # stop_handler = CommandHandler('stop', call_stop)

        # step5.Dispatcher에 Handler를 추가
        dispatcher.add_handler(balance_handler)
        dispatcher.add_handler(wait_handler)
        dispatcher.add_handler(target_handler)
        dispatcher.add_handler(log_handler)
        dispatcher.add_handler(price_handler)
        # dispatcher.add_handler(stop_handler)

        # step6.Updater 실시간 입력 모니터링 시작(polling 개념)
        updater.start_polling()
    except:
        logger.log(40, 'Exception Raised!')
        logger.log(40, e)

# -----------------------------------------------------------------------------
# - Name : run_trading
# - Desc : 거래 실행 함수
# -----------------------------------------------------------------------------
def run_trading(upbit_api, qreal, target, qlog):
    # print('거래실행')
    logger.log(20, '거래실행')
    upbit.send_telegram_message('거래실행')

    while True:
        # print(datetime.now(), target['STATUS'])
        if not p1.is_alive()&p2.is_alive():
            # print('child process 에러발생')
            logger.log(20, 'child process 에러발생')
            upbit.send_telegram_message('child process 에러발생')
            listener.listener_end(qlog)
            sys.exit()
        # status에 따른 주문을 별도의 함수로 구성.
        if (target['STATUS'] == 1)or(target['STATUS'] == 11):
            # print('미체결 종목 취소 실행')
            logger.log(20, '미체결 종목 취소 실행')
            upbit.send_telegram_message('미체결 종목 취소 실행')
            rtn_wait_order = upbit_api.get_order_list(state='wait')
            if len(rtn_wait_order) != 0:
                for each_order in rtn_wait_order:
                    if each_order['side'] == 'ask':  # 매도주문은 제외 (ask : 매도, bid : 매수)
                        continue
                    else:
                        rtn_order_cancel = upbit_api.cancel_order(each_order['uuid'])
                        # print(f'미체결 종목 취소\n{rtn_order_cancel}')
                        logger.log(20, f'미체결 종목 취소\n{rtn_order_cancel}')
                        upbit.send_telegram_message(f'미체결 종목 취소\n{rtn_order_cancel}')
                        time.sleep(0.1)
            else:
                logger.log(20, '미체결 대상 종목 없음')
                upbit.send_telegram_message('미체결 대상 종목 없음')
            target['STATUS'] = 0
        if (target['STATUS'] == 2)or(target['STATUS'] == 12):
            # print('보유종목 전량 매도')
            logger.log(20, '보유종목 전량 매도')
            upbit.send_telegram_message('보유종목 전량 매도')
            balances_raw = upbit_api.get_balances()
            balances = []
            for balance in balances_raw:
                if balance['currency'] in ['KRW', 'CPT']:
                    continue
                else:
                    balances.append(balance)
            if len(balances) > 0:
                # print('매도주문 실행')
                for balance in balances:
                    code_coin = f'{balance["unit_currency"]}-{balance["currency"]}'
                    rtn_order_sell = upbit_api.sell_market_order(code_coin,balance['balance'])
                    logger.log(20, f'보유종목 매도\n{rtn_order_sell}')
                    upbit.send_telegram_message(f'보유종목 매도\n{rtn_order_sell}')
                    time.sleep(0.1)
            else:
                logger.log(20, '매도 대상 종목 없음')
                upbit.send_telegram_message('매도 대상 종목 없음')
            target['STATUS'] = 0
        # qreal에 값이 들어올때까지 여기서 대기하다가 값 들어오면 그 다음 실행함.
        data = qreal.get()
        if (int(datetime.now().strftime('%H%M%S'))>=90000)&(int(datetime.now().strftime('%H%M%S'))<=100100):
            continue
        if data['ty'] == 'trade':
            if not p1.is_alive() & p2.is_alive():
                # print('child process 에러발생')
                logger.log(20, 'child process 에러발생')
                upbit.send_telegram_message('child process 에러발생')
                listener.listener_end(qlog)
                sys.exit()
            # list_coins를 중간에 변경하면 target에 없는 coin의 websocket이 들어올 수 있어 이를 제외.
            if data['cd'] in target.keys():
                target_coin = target[data['cd']]
            else:
                continue
            if target_coin[5]:
                continue
            if (target_coin[2] <= data['tp']*0.999)&(target_coin[2] >= data['tp']*0.999):
                remaining_asset = upbit_api.get_balance('KRW')
                order_value = target_coin[3]
                if order_value > remaining_asset:
                    # print(f'잔고부족으로 미실행\n{target_coin}')
                    continue
                else:
                    # print(f'주문실행\n{target_coin}')
                    rtn_order_buy = upbit_api.buy_market_order(target_coin[0],target_coin[3])
                    logger.log(20, f'주문실행 \n{rtn_order_buy}')
                    upbit.send_telegram_message(f'주문실행 \n{rtn_order_buy}')
                    target_coin[5] = 1
                    target[data['cd']] = target_coin
                    time.sleep(0.1)
                    # print('target 저장')
                    time1 = datetime.now()
                    if int(time1.strftime("%H%M%S")) < 93000:
                        file_target = f'./target/target_{(time1 - relativedelta(days=1)).strftime("%y%m%d")}.pickle'
                    else:
                        file_target = f'./target/target_{time1.strftime("%y%m%d")}.pickle'
                    with open(file_target, 'wb') as handle:
                        pickle.dump(dict(target), handle, protocol=pickle.HIGHEST_PROTOCOL)

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
        # print('main 실행')
        logger.log(20, 'main 실행')
        upbit.send_telegram_message('main 실행')
        # upbit.send_telegram_message('main 실행')
        qreal = mp.Queue()
        manager = mp.Manager()
        target = manager.dict({'STATUS':0, 'list_coins':[], 'value_per_trade':0, 'date':0})

        upbit_api = pyupbit.Upbit(upbit.access_key, upbit.secret_key)

        p1 = mp.Process(name="Price_Receiver", target=producer1, args=(qreal, target, qlog), daemon=True)
        p1.start()
        logger.log(20, 'Websocket Process 실행')
        p2 = mp.Process(name="Target_Receiver", target=producer2, args=(target, qlog), daemon=True)
        p2.start()
        logger.log(20, 'target Process 실행')
        p3 = mp.Process(name='Telegram', target=producer3, args=(upbit_api,target), daemon=True)
        p3.start()
        logger.log(20, 'telegram Process 실행')

        # asyncio.run(run_trading(real, target, qlog))

        run_trading(upbit_api, qreal, target, qlog)

    except KeyboardInterrupt:
        logger.log(40,"KeyboardInterrupt Exception 발생!")
        logger.log(40, traceback.format_exc())
        upbit.send_telegram_message("KeyboardInterrupt Exception 발생!")
        listener.listener_end(qlog)
        sys.exit()

    except Exception:
        logger.log(40, "Exception 발생!")
        logger.log(40, traceback.format_exc())
        upbit.send_telegram_message("Exception 발생!")
        listener.listener_end(qlog)
        sys.exit()