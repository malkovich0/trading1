import pickle

import pyupbit

status = 1
# list_coin_code = ['KRW-BTC','KRW-ETH','KRW-DOGE']
# list_coin_code = ['KRW-BTC','KRW-ETH']
list_coin_code =pyupbit.get_tickers('KRW')[:20]
# print(list_coin_code)
value_per_trade = 200000
case_dic = {'STATUS':status,'list_coin_code':list_coin_code, 'value_per_trade':value_per_trade}
with open('./info/trade_info.pickle','wb') as handle:
    pickle.dump(case_dic, handle, protocol=pickle.HIGHEST_PROTOCOL)