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
def producer1(qreal):
    # logger = upbit.Log().config_queue_log(qlog, 'websocket')
    asyncio.run(main_websocket(qreal))

async def main_websocket(qreal):
    try:
        # print('websocket 실행')
        # logger.log(20,'websocket 실행')
        # upbit.send_telegram_message('websocket 실행')
        # target이 저장됐는지 확인
        # date가 오늘인지 확인. (확인 후 10초 waiting)
        # while True:
            # if int(target['date']) == int(datetime.now().strftime('%y%m%d')):
            #     # print('일자확인완료')
            #     logger.log(20, f'target일자 확인 {target["date"]}')
            #     await asyncio.sleep(10)
            #     if len(target) > 4:
            #         break
            # else:
            #     await asyncio.sleep(10)
            # await asyncio.sleep(1)
        subscribe_items = ['KRW-BTC','KRW-ETH']
        # logger.log(20, f'websocket 조회종목 ({len(subscribe_items)}) : {subscribe_items}')
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
            # logger.log(20,f'websocket 구독시작\n{subscribe_items}')
            # upbit.send_telegram_message(f'websocket 구독시작\n{subscribe_items}')
            while True:
                # if subscribe_items != target['list_coins']:
                #     logger.log(20,f'websocket 재실행')
                #     await main_websocket(qreal, target, logger)
                data = await websocket.recv()
                data = json.loads(data)
                qreal.put(data)
                if data['ty'] == 'trade':
                    code_coin = data['cd']
                    str_month = datetime.now().strftime('%y%W')
                    csv_file = f'./data/tickdata_{code_coin}_{str_month}_r1.csv'
                    save_to_csv(data, csv_file)

    except Exception as e:
        # print('websocket Error')
        # logger.log(40, 'Exception Raised!')
        # logger.log(40,e)
        await main_websocket(qreal)

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

if __name__ == "__main__":
    # listener = upbit.Log()
    # listener.listener_start('test', 'listener', qlog)
    # logger = upbit.Log().config_queue_log(qlog, 'main')

    try:
        # ---------------------------------------------------------------------
        # Logic Start!
        # ---------------------------------------------------------------------
        # 웹소켓 시작
        qreal = mp.Queue()

        # upbit_api = pyupbit.Upbit(upbit.access_key_pc, upbit.secret_key_pc)

        p1 = mp.Process(name="Price_Receiver", target=producer1, args=(qreal, ), daemon=True)
        p1.start()
        p1.join()

    except KeyboardInterrupt:
        # logger.log(40,"KeyboardInterrupt Exception 발생!")
        # logger.log(40, traceback.format_exc())
        # upbit.send_telegram_message("KeyboardInterrupt Exception 발생!")
        # listener.listener_end(qlog)
        sys.exit()

    except Exception:
        # logger.log(40, "Exception 발생!")
        # logger.log(40, traceback.format_exc())
        # upbit.send_telegram_message("Exception 발생!")
        # listener.listener_end(qlog)
        sys.exit()