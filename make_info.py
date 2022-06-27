import pickle

import pyupbit
import time

status = 1
# list_coin_code = ['KRW-BTC','KRW-ETH','KRW-DOGE']
# list_coin_code = ['KRW-BTC','KRW-XRP']
list_coin_code =pyupbit.get_tickers('KRW')
# list_coin_code_new = []
# for code_coin in list_coin_code:
#     print(code_coin)
#     df_candle = pyupbit.get_ohlcv(code_coin, count=10)
    # if df_candle is None:
    #     continue
    # else:
    #     list_coin_code_new.append(code_coin)
    # print(df_candle)
    # time.sleep(0.1)
# print(list_coin_code)
value_per_trade = 200000
# print(len(list_coin_code_new))
# print(list_coin_code_new)
case_dic = {'STATUS':status,'list_coin_code':list_coin_code, 'value_per_trade':value_per_trade}
with open('./info/trade_info.pickle','wb') as handle:
    pickle.dump(case_dic, handle, protocol=pickle.HIGHEST_PROTOCOL)