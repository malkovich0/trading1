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

from csv import DictWriter

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
                if data['ty'] == 'trade':
                    code_coin = data['cd']
                    str_month = datetime.now().strftime('%y%m')
                    csv_file = f'./data/tickdata_{code_coin}_{str_month}.csv'
                    save_to_csv(data, csv_file)

    except Exception as e:
        # print('websocket Error')
        logger.log(40, 'Exception Raised!')
        logger.log(40,e)
        await main_websocket(qreal, target, logger)

def save_to_csv(dict_data, csv_file):
    field_names = ['tp','tv','ab','td','ttm']
    dict_data = {key:dict_data[key] for key in field_names}
    if os.path.isfile(csv_file):
        with open(csv_file, 'a') as f_object:
            dictwriter_object = DictWriter(f_object, fieldnames=field_names)
            dictwriter_object.writerow(dict_data)
            f_object.close()
    else:
        field_names = dict_data.keys()
        with open(csv_file, 'w') as f_object:
            dictwriter_object = DictWriter(f_object, fieldnames=field_names)
            dictwriter_object.writeheader()
            dictwriter_object.writerow(dict_data)
            f_object.close()



# -----------------------------------------------------------------------------
# - Name : main_target
# - Desc : 거래대상 생성 함수
# -----------------------------------------------------------------------------
def producer2(target, qlog, target_trade):
    logger = upbit.Log().config_queue_log(qlog, 'target')
    asyncio.run(main_target(target, logger, target_trade))

async def main_target(target, logger, target_trade):
    try:
        # print('target 실행')
        logger.log(20,'target 실행')
        upbit.send_telegram_message('target 실행')
        making_trading_variables = wait_trading_variables(target, logger)
        making_target = wait_trading_target(target, logger)
        # making_target_trade = wait_trading_target_detail(target,logger,target_trade)
        await asyncio.gather(making_trading_variables, making_target)
    except Exception as e:
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
                for code_coin in target['list_coins']:
                    if code_coin in target.keys():
                        target_list_temp = target[code_coin]
                        target_list_temp[6] = define_high_price(code_coin)
                        target[code_coin] = target_list_temp
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
                for code_coin in target['list_coins']:
                    if code_coin in target.keys():
                        target_list_temp = target[code_coin]
                        target_list_temp[6] = define_high_price(code_coin)
                        target[code_coin] = target_list_temp
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
            for code_coin in target['list_coins']:
                if code_coin in target.keys():
                    target_list_temp = target[code_coin]
                    target_list_temp[6] = define_high_price(code_coin)
                    target[code_coin] = target_list_temp

        # 정상작동 중 10시가 되어 여기 넘어오면 target이 이미 정의되어 있다.
        elif int(time_start.strftime("%H%M%S")) >= 100000:
            time_target = time_start.replace(hour=9, minute=10, second=0)
            file_target = f'./target/target_{time_target.strftime("%y%m%d")}.pickle'
            time_next_target = (time_start + relativedelta(days=1)).replace(hour=9, minute=10, second=0)
            # 프로그램 실행 중
            if len(target)>4:
                # 현재 시점 기준으로 고가 저장하기.
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
                    for code_coin in target['list_coins']:
                        if code_coin in target.keys():
                            target_list_temp = target[code_coin]
                            target_list_temp[6] = define_high_price(code_coin)
                            target[code_coin] = target_list_temp
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

async def wait_trading_target_detail(target, logger, target_trade):
    try:
        while True:
            # target_trade가 없고 저장된 파일이 있으면 불러오는 함수
            this_target_time = datetime.now().replace(minute=0, second=0)
            file_target_detail = f'./target/target_{this_target_time.strftime("%y%m%d%H")}.pickle'
            if len(target_trade) == 0:
                if os.path.isfile(file_target_detail):
                    logger.log(20,f'저장된 target 가져오기\n{file_target_detail}')
                    upbit.send_telegram_message(f'저장된 target 가져오기\n{file_target_detail}')
                    with open(file_target_detail, 'rb') as handle:
                        target_trade_list = pickle.load(handle)
                        for target_detail_temp in target_trade_list:
                            target_trade.append(target_detail_temp)
            next_target_time = (datetime.now()+relativedelta(hours=1)).replace(minute=0,second=0)
            logger.log(20,f'target_detail대기 : {next_target_time}')
            await asyncio.sleep((next_target_time - datetime.now()).total_seconds())
            # 거래제외시간설정.
            if int(next_target_time.strftime('%H')) in [9,10,11,12,13,14,15]:
                continue
            # {code_coin, uuid_buy, uuid_sell, status_no, start_time, value, buy_price, high_price, stoploss_ratio}
            # coin_list 종목에 대해서 target_detail 생성. (함수 생성, value 정의)
            for code_coin in target['list_coins']:
                target_detail_temp = define_trading_target_detail(code_coin, target['value_per_trade']/20)
                target_trade.append(target_detail_temp)
                logger.log(20,f'target_detail저장 : {target_trade}\n{target_detail_temp}')
            # target_trade 1일 이상 경과한 종목 제거
            for i in range(len(target_trade)-1):
                # 시간기준이 아니라 status 종료 기준으로 평가. (시간기준은 run_trading에서)
                if target_trade[i]['status_no'] == 99:
                # if int(datetime.now().strftime('%y%m%d%H')) - int(target_trade[i]['start_time']) >= 100:
                    target_trade = target_trade[i+1:]
                #     continue
                else:
                #     target_trade = target_trade[i:]
                    break
            with open(file_target_detail,'wb') as handle:
                pickle.dump(target_trade, handle, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception as e:
        logger.log(40, 'Exception Raised!')
        logger.log(40, e)

def define_trading_target_detail(code_coin, value):
    start_time = int(datetime.now().strftime('%y%m%d%H'))
    end_time = int((datetime.now()+relativedelta(days=1)).replace(minute=0,second=0).strftime('%y%m%d%H'))
    detail_temp = {'code_coin':code_coin,'uuid_buy':None,'uuid_sell':None,'status_no':None,'start_time':start_time,
                   'end_time':end_time, 'value':None, 'balance':None,
                   'buy_price':None,'sell_price':None,'high_price':None,'stoploss_ratio':None}
    df_candle = pyupbit.get_ohlcv(code_coin, interval='minute60', count=193)
    if int(df_candle.index[-1].strftime('%y%m%d%H')) == start_time:
        dict_ohlcv = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'value': 'sum', 'volume': 'sum'}
        df_candle = df_candle.resample('24H', origin=df_candle.index[0]).apply(dict_ohlcv)
        # range 계산
        price_range = np.array(df_candle['high'] - df_candle['low'])[-2]
        # k_value 정의
        k_value = \
        np.maximum(0.5, np.abs(df_candle['open'] - df_candle['close']) / (df_candle['high'] - df_candle['low']))[-2]
        target_price = df_candle.close.iloc[-2] + k_value * price_range
        open_price = df_candle.open.iloc[-1]
        today_high = df_candle.high.iloc[-1]
        target_vol = 0.1
        stoploss = 0.1  # k_value * price_range / open_price
        if stoploss <= target_vol:
            value_order = value
        else:
            value_order = value / stoploss * target_vol
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
        #     filter_value = 3
        # elif volume_ma[-2] < volume_minimum:
        #     filter_value = 3
        # elif volume_last < volume_minimum:
        #     filter_value = 3
        elif price_ma[-2] > price_last:
            filter_value = 99
        #    elif volume_ma[-2] < volume_last:
        #        return None
        else:
            pass

        detail_temp['status_no'] = filter_value
        detail_temp['value'] = value_order
        detail_temp['buy_price'] = target_price
        detail_temp['sell_price'] = df_candle.close.iloc[-2] - k_value * price_range
        detail_temp['high_price'] = today_high
        detail_temp['stoploss_ratio'] = stoploss
    else:
        logger.log(20,f'현재시점 주가 가져오지 못함 : {start_time}, {int(df_candle.index[-1].strftime("%y%m%d%H"))}')
        time.sleep(1)
        detail_temp = define_trading_target_detail(code_coin, value)
    # {code_coin, uuid_buy, uuid_sell, status_no, start_time, end_time, value, buy_price, sell_price, high_price, stoploss_ratio}
    return detail_temp

def define_trading_target(code_coin, value):
    while True:
        df_candle = pyupbit.get_ohlcv(code_coin, count=30)
        if df_candle.index[-1].strftime('%y%m%d') == datetime.now().strftime('%y%m%d'):
            # range 계산
            price_range = np.array(df_candle['high'] - df_candle['low'])[-2]

            # k_value 정의
            k_value = np.maximum(0.5, np.abs(df_candle['open'] - df_candle['close']) / (df_candle['high'] - df_candle['low']))[-2]
            target_price = df_candle.close.iloc[-2] + k_value * price_range
            open_price = df_candle.open.iloc[-1]
            today_high = df_candle.high.iloc[-1]
            target_vol = 0.1
            stoploss = 0.1  #k_value * price_range / open_price
            if  stoploss <= target_vol:
                value_order = value
            else:
                value_order = value / stoploss * target_vol

            # value_order = value

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
            #     filter_value = 3
            # elif volume_ma[-2] < volume_minimum:
            #     filter_value = 3
            # elif volume_last < volume_minimum:
            #     filter_value = 3
            elif price_ma[-2] > price_last:
                filter_value = 3
            #    elif volume_ma[-2] < volume_last:
            #        return None
            else:
                pass
            break
        else:
            continue
    # 종목, 날짜, 목표가, 주문총액, 주문종류, 주문상태, 당일최고가, 손전폭
    trading_target_temp = [code_coin, datetime.today().strftime("%y%m%d"), target_price, value_order, 'buy', filter_value, today_high, stoploss]
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

def define_high_price(code_coin):
    df_candle = pyupbit.get_ohlcv(code_coin, count=5)
    return df_candle.high.iloc[-1]

# -----------------------------------------------------------------------------
# - Name : main_telegram
# - Desc : telegram 연결 관리 함수
# -----------------------------------------------------------------------------
def producer3(upbit_api, target, target_trade):
    main_telegram(upbit_api, target, target_trade)

def main_telegram(upbit_api, target, target_trade):
    try:
        # step2.Updater(유저의 입력을 계속 모니터링하는 역할), Dispatcher
        updater = Updater(token=upbit.telegram_token, use_context=True)
        dispatcher = updater.dispatcher

        # step3./start 명령어가 입력되었을 때의 함수 정의
        # help 어떤 명령어가 있는지 확인.
        def call_help(update, context):
            msg = '/balance : 잔고조회\n/wait : 미체결조회\n/target : target조회\n/target_trade : target_trade조회\n/log : 당일 log출력\n/price target : target의 현재가 조회\n/price 종목코드 : 종목 현재가 조회'
            context.bot.send_message(chat_id=update.effective_chat.id, text=msg)

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

        def call_target_trade(update, context):
            msg = target_trade
            context.bot.send_message(chat_id=update.effective_chat.id, text=f'<target_trade 조회>\n{msg}')

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
        help_handler = CommandHandler('help', call_help)
        balance_handler = CommandHandler('balance', call_balance)
        wait_handler = CommandHandler('wait', call_wait_order)
        target_handler = CommandHandler('target', call_target)
        target_trade_handler = CommandHandler('target_trade', call_target_trade)
        log_handler = CommandHandler('log', call_log)
        price_handler = CommandHandler('price', call_price)
        # stop_handler = CommandHandler('stop', call_stop)

        # step5.Dispatcher에 Handler를 추가
        dispatcher.add_handler(help_handler)
        dispatcher.add_handler(balance_handler)
        dispatcher.add_handler(wait_handler)
        dispatcher.add_handler(target_handler)
        dispatcher.add_handler(target_trade_handler)
        dispatcher.add_handler(log_handler)
        dispatcher.add_handler(price_handler)
        # dispatcher.add_handler(stop_handler)

        # step6.Updater 실시간 입력 모니터링 시작(polling 개념)
        updater.start_polling()
    except Exception as e:
        logger.log(40, 'Exception Raised!')
        logger.log(40, e)

# -----------------------------------------------------------------------------
# - Name : run_trading
# - Desc : 거래 실행 함수
# -----------------------------------------------------------------------------
def call_buy_order(upbit_api, code_coin, value):
    remaining_asset = upbit_api.get_balance('KRW')
    if value <= remaining_asset:
        logger.log(20, f'detail매수주문 : {code_coin}, {value}')
        upbit.send_telegram_message(f'detail매수주문 : {code_coin}, {value}')
        order_uuid = None
        # rtn_order_buy = upbit_api.buy_market_order(code_coin, value)
        # time.sleep(0.5)
        # order_uuid = rtn_order_buy['uuid']
        # rtn_order_result = upbit_api.get_order(order_uuid)
        # for each_result in rtn_order_result['trades']:
        #     msg_result = f'종목 : {each_result["market"]}\n가격 : {each_result["price"]}\n거래량 : {each_result["volume"]}'
        #     logger.log(20, f'매수주문 실행\n{each_result}')
        #     upbit.send_telegram_message(f'매수주문 실행\n{msg_result}')
        status_no = 10
    else:
        order_uuid = None
        status_no = 0
    return status_no, order_uuid

def call_sell_order(upbit_api, code_coin, balance):
    logger.log(20, f'detail매수주문 : {code_coin}, {balance}')
    upbit.send_telegram_message(f'detail매수주문 : {code_coin}, {balance}')
    # rtn_order_sell = upbit_api.sell_market_order(code_coin, balance)
    # time.sleep(0.5)
    # order_uuid = rtn_order_sell['uuid']
    status_no = 20
    order_uuid = None
    # rtn_order_result = upbit_api.get_order(order_uuid)
    # for each_result in rtn_order_result['trades']:
    #     msg_result = f'종목 : {each_result["market"]}\n가격 : {each_result["price"]}\n거래량 : {each_result["volume"]}'
    #     logger.log(20, f'보유종목 매도\n{each_result}')
    #     upbit.send_telegram_message(f'보유종목 매도\n{msg_result}')
    return status_no, order_uuid

def run_trading(upbit_api, qreal, target, qlog, target_trade):
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
    #     data = qreal.get()
    #     if data['ty'] == 'trade':
    #         code_coin = data['cd']
    #         no_target_trade = len(target_trade)
    #         for i in range(no_target_trade):
    #             target_detail = target_trade[i].copy()
    #             if target_detail['code_coin'] == code_coin:
    #                 # 최근가가 고가면 저장하기.
    #                 if data['tp'] > target_detail['high_price']:
    #                     target_detail['high_price'] = data['tp']
    #                 # 거래종료 시간 (매수상태면 매도하고 status_no=99. 매수전or매도면 status_no=99.)
    #                 if target_detail['end_time'] <= int(datetime.now().strftime('%y%m%d%H')):
    #                     if target_detail['status_no'] == 10:
    #                         target_detail['status_no'], target_detail['uuid_sell'] = call_sell_order(upbit_api, target_detail['code_coin'], target_detail['balance'])
    #                         target_detail['status_no'] = 99
    #                     else:
    #                         target_detail['status_no'] = 99
    #                 # 주문 미실행.
    #                 elif target_detail['status_no'] == 0:
    #                     if (target_detail['buy_price'] <= data['tp']*1.01)&(target_detail['buy_price'] >= data['tp']*0.999):
    #                         target_detail['status_no'], target_detail['uuid_buy'] = call_buy_order(upbit_api, target_detail['code_coin'], target_detail['value'])
    #                         # 잔고평가해서 매수하고 메시지 남기는 함수 구성. rtn에 따라 매수했다면 status_no 변경.
    #                 # 매수주문 상태. stoploss or sell_price
    #                 elif target_detail['status_no'] == 10:
    #                     if data['tp'] <= max(target_detail['sell_price'],
    #                                          target_detail['high_price'] * (1 - target_detail['stoploss_ratio'])) * 1.001:
    #                         target_detail['status_no'], target_detail['uuid_sell'] = call_sell_order(upbit_api,target_detail['code_coin'],target_detail['balance'])
    #                 # 매도주문 상태.
    #                 elif target_detail['status_no'] == 20:
    #                     continue
    #                 else:
    #                     logger.log(20,f'status_no check\n{target_detail}')
    #             else:
    #                 continue
    #             target_trade[i] = target_detail


        # target_trade관련
        # status_no 확인하면서 매수, 매도 판단.
        # status에 따른 주문을 별도의 함수로 구성.
        if (target['STATUS'] == 1)or(target['STATUS'] == 11):
            #
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
                        time.sleep(0.1)
                        logger.log(20, f'미체결 종목 취소\n{rtn_order_cancel}')
                        upbit.send_telegram_message(f'미체결 종목 취소\n{rtn_order_cancel}')

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
                    time.sleep(0.5)
                    rtn_order_result = upbit_api.get_order(rtn_order_sell['uuid'])
                    for rtn_order_result in rtn_order_result['trades']:
                        msg_result = f'종목 : {rtn_order_result["market"]}\n가격 : {rtn_order_result["price"]}\n거래량 : {rtn_order_result["volume"]}'
                        logger.log(20, f'보유종목 매도\n{rtn_order_result}')
                        upbit.send_telegram_message(f'보유종목 매도\n{msg_result}')
                    time.sleep(0.1)
                rtn_daily = daily_return()
                upbit.send_telegram_message(rtn_daily)
            else:
                logger.log(20, '매도 대상 종목 없음')
                upbit.send_telegram_message('매도 대상 종목 없음')
            target['STATUS'] = 0
        # qreal에 값이 들어올때까지 여기서 대기하다가 값 들어오면 그 다음 실행함.
        data = qreal.get()
        # if (int(datetime.now().strftime('%H%M%S'))>=90000)&(int(datetime.now().strftime('%H%M%S'))<=100100):
        #     continue
        if data['ty'] == 'trade':
            if not p1.is_alive() & p2.is_alive():
                # print('child process 에러발생')
                logger.log(20, 'child process 에러발생')
                upbit.send_telegram_message('child process 에러발생')
                listener.listener_end(qlog)
                sys.exit()

            # target_trade관련 함수.
            code_coin = data['cd']
            no_target_trade = len(target_trade)
            for i in range(no_target_trade):
                target_detail = target_trade[i].copy()
                if target_detail['code_coin'] == code_coin:
                    # 최근가가 고가면 저장하기.
                    if data['tp'] > target_detail['high_price']:
                        target_detail['high_price'] = data['tp']
                    # 거래종료 시간 (매수상태면 매도하고 status_no=99. 매수전or매도면 status_no=99.)
                    if target_detail['end_time'] <= int(datetime.now().strftime('%y%m%d%H')):
                        if target_detail['status_no'] == 10:
                            target_detail['status_no'], target_detail['uuid_sell'] = call_sell_order(upbit_api, target_detail['code_coin'], target_detail['balance'])
                            target_detail['status_no'] = 99
                        else:
                            target_detail['status_no'] = 99
                    # 주문 미실행.
                    elif target_detail['status_no'] == 0:
                        if (target_detail['buy_price'] <= data['tp']*1.01)&(target_detail['buy_price'] >= data['tp']*0.999):
                            target_detail['status_no'], target_detail['uuid_buy'] = call_buy_order(upbit_api, target_detail['code_coin'], target_detail['value'])
                            # 잔고평가해서 매수하고 메시지 남기는 함수 구성. rtn에 따라 매수했다면 status_no 변경.
                    # 매수주문 상태. stoploss or sell_price
                    elif target_detail['status_no'] == 10:
                        if data['tp'] <= max(target_detail['sell_price'],
                                             target_detail['high_price'] * (1 - target_detail['stoploss_ratio'])) * 1.001:
                            target_detail['status_no'], target_detail['uuid_sell'] = call_sell_order(upbit_api,target_detail['code_coin'],target_detail['balance'])
                    # 매도주문 상태.
                    elif target_detail['status_no'] == 20:
                        continue
                    # 거래제외or 완료.
                    elif target_detail['status_no'] == 99:
                        continue
                    else:
                        logger.log(20,f'status_no check\n{target_detail}')
                else:
                    continue
                target_trade[i] = target_detail


            # list_coins를 중간에 변경하면 target에 없는 coin의 websocket이 들어올 수 있어 이를 제외.
            if data['cd'] in target.keys():
                target_coin = target[data['cd']]
            else:
                continue
            #  더 높은 가격이면 저장하기
            if data['tp'] > target_coin[6]:
                target_coin[6] = data['tp']
                target[data['cd']] = target_coin
            #  이미 매수한 종목이면 매도해야하는지 확인
            if target_coin[5] == 1:
                if data['tp'] <= target_coin[6]*(1-target_coin[7])*1.001:
                    balance = upbit_api.get_balance(target_coin[0])
                    rtn_order_sell = upbit_api.sell_market_order(target_coin[0],balance)
                    target_coin[5] = 2
                    target[data['cd']] = target_coin
                    # print('target 저장')
                    time1 = datetime.now()
                    if int(time1.strftime("%H%M%S")) <= 100000:
                        file_target = f'./target/target_{(time1 - relativedelta(days=1)).strftime("%y%m%d")}.pickle'
                    else:
                        file_target = f'./target/target_{time1.strftime("%y%m%d")}.pickle'
                    with open(file_target, 'wb') as handle:
                        pickle.dump(dict(target), handle, protocol=pickle.HIGHEST_PROTOCOL)
                    # 매도 결과 저장.
                    time.sleep(0.5)
                    rtn_order_result = upbit_api.get_order(rtn_order_sell['uuid'])
                    for rtn_order_result in rtn_order_result['trades']:
                        msg_result = f'종목 : {rtn_order_result["market"]}\n가격 : {rtn_order_result["price"]}\n거래량 : {rtn_order_result["volume"]}'
                        logger.log(20, f'보유종목 매도\n{rtn_order_result}')
                        upbit.send_telegram_message(f'보유종목 매도\n{msg_result}')
                    time.sleep(0.1)
                continue
            #  매수하지 않은 종목이면 가격으로 평가
            elif target_coin[5]==0:
                if (int(datetime.now().strftime('%H%M%S')) >= 90000) & (int(datetime.now().strftime('%H%M%S')) <= 100100):
                    continue

                if (target_coin[2] <= data['tp']*1.01)&(target_coin[2] >= data['tp']*0.999):
                    remaining_asset = upbit_api.get_balance('KRW')
                    order_value = target_coin[3]
                    if order_value > remaining_asset:
                        # print(f'잔고부족으로 미실행\n{target_coin}')
                        continue
                    else:
                        # print(f'주문실행\n{target_coin}')
                        rtn_order_buy = upbit_api.buy_market_order(target_coin[0],target_coin[3])
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
                        # 매수결과 저장.
                        time.sleep(0.5)
                        rtn_order_result = upbit_api.get_order(rtn_order_buy['uuid'])
                        for rtn_order_result in rtn_order_result['trades']:
                            msg_result = f'종목 : {rtn_order_result["market"]}\n가격 : {rtn_order_result["price"]}\n거래량 : {rtn_order_result["volume"]}'
                            logger.log(20, f'매수주문 실행\n{rtn_order_result}')
                            upbit.send_telegram_message(f'매수주문 실행\n{msg_result}')
            #  매수 후에 매도한 경우.
            elif target_coin[5] == 2:
                continue
            else:
                continue

def daily_return():
    order_done = upbit_api.get_order_list(state='done')
    order_cancel = upbit_api.get_order_list(state='cancel')
    time_start = int((datetime.now()-relativedelta(days=1)).replace(hour=10, minute=0,second=1).strftime('%y%m%d%H%m'))
    time_end = int((datetime.now()).replace(hour=10, minute=0,second=1).strftime('%y%m%d%H%m'))
    trade_revenue = {}
    trade_return = {}
    for each_order in order_done+order_cancel:
        time_temp = each_order['created_at']
        time_temp = time_temp[2:4] + time_temp[5:7] + time_temp[8:10] + time_temp[14:16]+ time_temp[17:19]
        if (int(time_temp) >= int(time_start))&(int(time_temp)<=int(time_end)):
            result_temp = upbit_api.get_order(each_order['uuid'])
            code = result_temp['market']
            side = result_temp['side']
            if (side=='ask') & (int(time_temp[6:]) >= 1000) & (int(time_temp[6:]) <= 1005):
                continue
            value = sum(float(result_temp['trades'][i]['price'])*float(result_temp['trades'][i]['volume']) for i in range(len(result_temp['trades'])))
            if side == 'bid':
                value = value*-1
            fee = float(result_temp['paid_fee'])
            value_fee = value-fee
            if code in trade_return.keys():
                trade_return[code] = trade_return[code] + value_fee
            else:
                trade_return[code] = value_fee
            if side == 'bid':
                if code in trade_revenue.keys():
                    trade_revenue[code] = trade_revenue[code] - value
                else:
                    trade_revenue[code] = value*-1
            #
            # if (code not in trade_revenue.keys())&(side == 'bid'):
            #     trade_revenue[code] = value*-1
    msg_rtn = ''
    for code_coin in trade_revenue.keys():
        msg_rtn += f'{code_coin} : {trade_revenue[code_coin]:.1f}({trade_return[code_coin]:.1f}, {trade_return[code_coin]/trade_revenue[code_coin]*100:.2f}%) \n'
    return msg_rtn


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
        target_trade = manager.list()

        upbit_api = pyupbit.Upbit(upbit.access_key, upbit.secret_key)

        p1 = mp.Process(name="Price_Receiver", target=producer1, args=(qreal, target, qlog), daemon=True)
        p1.start()
        logger.log(20, 'Websocket Process 실행')
        p2 = mp.Process(name="Target_Receiver", target=producer2, args=(target, qlog, target_trade), daemon=True)
        p2.start()
        logger.log(20, 'target Process 실행')
        p3 = mp.Process(name='Telegram', target=producer3, args=(upbit_api,target, target_trade), daemon=True)
        p3.start()
        logger.log(20, 'telegram Process 실행')

        # asyncio.run(run_trading(real, target, qlog))

        run_trading(upbit_api, qreal, target, qlog, target_trade)

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