import pyupbit
import pandas as pd
from datetime import datetime
import time
# from module import upbit
import telegram
import pickle

tickers = pyupbit.get_tickers(fiat="KRW")
timeframe = 'minute1'#'minute1' #minute60 #day
# theCoin = tickers[0]
token_test = '5551503871:AAGpGczJlEXQeSUEMNUw5lXwhAKp0tUqPew'
chat_id = '5441242058'

def send_telegram_message(message):
    try:
        # 텔레그램 메세지 발송
        bot = telegram.Bot(token_test)
        res = bot.sendMessage(chat_id=chat_id, text=message)

        return res

    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception:
        raise


if __name__ == '__main__':
    # test_dic = pd.DataFrame()
    # with open(f'./data/test.pickle', 'wb') as handle:
    #     pickle.dump(test_dic, handle, protocol=pickle.HIGHEST_PROTOCOL)

    df_all = pd.DataFrame()
    to_date = datetime.now()
    # for i in range(len(tickers)):
    # for i in range(3):
    # theCoin = tickers[i]
    theCoin = tickers[0]
    try:
    # for theCoin in tickers:
        send_telegram_message(f'시작 : {theCoin}')

    #     print( i , theCoin)
        df_coin = pyupbit.get_ohlcv(ticker=theCoin, interval=timeframe, to=to_date)
        while True:
        # for _ in range(2):
            time.sleep(0.3)
            timeToGet = df_coin.index[0]
            df_temp = pyupbit.get_ohlcv(ticker=theCoin, interval=timeframe, to=timeToGet)
            df_coin = pd.concat([df_temp,df_coin])
            if df_temp is None:
                break
            if len(df_temp)<200:
                break
            if int(df_coin.index[-1].strftime('%d')) == 1:
                send_telegram_message(f'진행중 : {df_coin.index[-1].strftime("%y%m%d")}')
        df_coin['date'] = df_coin.index
        df_coin['coin'] = theCoin
        # df_coin
        df_all = pd.concat([df_all, df_coin])
        df_all.reset_index(inplace=True, drop=True)
        with open(f'./data/data_coin_minute1_{theCoin}.pickle','wb') as handle:
            pickle.dump(dict(df_all), handle, protocol=pickle.HIGHEST_PROTOCOL)
        # df_all.to_pickle(f'./data/data_coin_minute1_{theCoin}.pickle')
        send_telegram_message(f'종료 : {theCoin}')

    # else:
    # file_target = f'./target/target_{time1.strftime("%y%m%d")}.pickle'
    # with open(file_target, 'wb') as handle:
    #     pickle.dump(dict(target), handle, protocol=pickle.HIGHEST_PROTOCOL)

    except:
        send_telegram_message(f'Error발생 : {theCoin}')