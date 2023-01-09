import numpy as np
import pandas as pd
from scipy.optimize import brute
import pyupbit
from datetime import datetime
import time
from dateutil.relativedelta import relativedelta
import warnings
from typing import *
import pandas_ta as ta
import talib
import empyrical as ep
from tqdm import tqdm
import matplotlib.pyplot as plt
warnings.filterwarnings('ignore')
import joblib
import quantstats as qs
import pyarrow.csv as pc
from pathlib import Path
import pickle

class Action:
    """
    Abstract class for Order, OrderCancellation
    """
    def __init__(self):
        pass
class Strategy:
    """
    Abstract method for generating user-defined trading strategies with certis
    """
#     def __init__(self, config, name="CertisStrategy"):
#         self.config = config
#         self.name = name

    def calculate(self, data):
        return data

#     def calculate(self, data: pd.DataFrame):
#         return self._calculate(data)

#     def execute(self, state_dict: Dict[str, Any]) -> List[Action]:
#         raise NotImplementedError
def _power10(x):
    return np.power(10,x)
def _sma(series, n):
    return series.rolling(n).mean()
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
def find_n_max(m, n):
    sorted_nums = sorted(m, reverse=True)
    return sorted_nums[n-1]


# class Backtest():
#     def __init__(self, datetime_start, datetime_end, code_coin, timeframe='minute10'):
#         self.data_raw = define_data(datetime_start - relativedelta(days=10), datetime_end, code_coin,
#                                    timeframe=timeframe)
#         self.data_d = self.data_raw[['close']].resample('24H', origin=self.data_raw.index[0].replace(hour=9)).apply(
#             {'close': 'last'})
#         self.data_d.dropna(inplace=True) # 휴일제거
#         self.data_d['return'] = np.log10(self.data_d['close'] / self.data_d['close'].shift(1))
#         self.data_d = self.data_d[['return']].loc[datetime_start:datetime_end]
#         self.trading_fee = 0.1
#         self.data_dict = {}
#
#     def cal_strategy(self, name, strategy_cls: type, strategy_list: None):
#         self.name = name
#         self.strategy_list = strategy_list
#         data_d_temp = self.data_d.copy()
#         data_raw_temp = self.data_raw.copy()
#         self.strategy: Strategy = strategy_cls(data_d_temp, data_raw_temp, self.strategy_list, self.trading_fee)
#         self.data_dict[name], self.data_d[name] = self.strategy.calculate()
#
#     def analyze_result(self, add=False):
#         if add:
#             data_dict = self.data_dict
#         else:
#             data_dict = None
#         return _analyze_result(self.data_d, data_dict)
#
#     def plot_result(self):
#         _plot_result(self.data_d)
#
#     def corr_result(self):
#         return self.data_d.corr()
#
#     def analyze_result_time(self, name=None):
#         # name이 none일때는 전체 strategy에 대해서 출력되게 수정 필요
#         data = self.data_dict[name]
#         return _analyze_result_time(data)
def make_stock_list(timeframe):
    import os
    if timeframe == 'hourly':
        list_existing = os.listdir('./data/hr/')
    elif timeframe == 'minute10':
        list_existing = os.listdir('./data/10m/')
    elif timeframe == 'minute5':
        list_existing = os.listdir('./data/5m/')
    elif timeframe == 'minute1':
        list_existing = os.listdir('./data/1m/')
    list_existing = [ext[1:7] for ext in list_existing]
    return list_existing
import dask.dataframe as dd

def reading_csv_dd(file_name, datetime_start, datetime_end):
    file = pd.read_csv(file_name, index_col=0)
    file.sort_values(by='date', ascending=True, inplace=True)
    file = file[(file['date'] >= int((datetime_start - relativedelta(days=1)).strftime('%Y%m%d%H%M'))) & (
                file['date'] <= int(datetime_end.strftime('%Y%m%d%H%M')))]
    file['code_stock'] = str(file_name)[-10:-4]
    # file['date'] = file['date'].astype(np.int32)
    # file['time'] = file['time'].astype(np.int16)
    # file['open'] = file['open'].astype(np.int32)
    # file['high'] = file['high'].astype(np.int32)
    # file['low'] = file['low'].astype(np.int32)
    # file['close'] = file['close'].astype(np.int32)
    # file['volume'] = file['volume'].astype(np.int32)
    file.drop(['value'], axis=1, inplace=True)
    return file.to_numpy()


def reading_csv(file_name, datetime_start, datetime_end):
    file = pd.read_csv(file_name, index_col=0)
    file.sort_values(by='date', ascending=True, inplace=True)
    file = file[(file['date'] >= int((datetime_start - relativedelta(days=1)).strftime('%Y%m%d%H%M'))) & (
                file['date'] <= int(datetime_end.strftime('%Y%m%d%H%M')))]
    file['code_stock'] = str(file_name)[-10:-4]
    return file[['date','time','open','high','low','close','volume','value','code_stock']]

class Backtest_Stocks():
    def __init__(self, datetime_start, datetime_end, code_stock='all', timeframe='minute10',option='pd'):
        # code_coin이 all인 경우에 전체 종목에 대해 sorting하도록 처리
        if code_stock == 'all':
            if timeframe == 'minute10':
                files = Path("./data/10m/").rglob("*.csv")
            if timeframe == 'minute5':
                files = Path("./data/5m/").rglob("*.csv")
                files = [i for i in files if str(i)[-5] == '0']
            if timeframe == 'minute1':
                files = Path("./data/1m/").rglob("*.csv")
            # files = list(files)
            data_raw = [reading_csv(file,datetime_start,datetime_end) for file in files]
            data_raw = pd.concat(data_raw)
            self.data_raw = data_raw
            # self.data_raw = data_raw[(data_raw['date']>=int((datetime_start-relativedelta(days=1)).strftime('%Y%m%d%H%M')))&(data_raw['date']<=int(datetime_end.strftime('%Y%m%d%H%M')))]
        elif isinstance(code_stock, list):
            if timeframe == 'minute10':
                data_raw = [reading_csv(f'./data/10m/A{file}.csv',datetime_start,datetime_end) for file in code_stock]
            elif timeframe == 'minute5':
                if option == 'pd':
                    data_raw = [reading_csv(f'./data/5m_modi/A{file}.csv', datetime_start, datetime_end) for file in
                               code_stock]
                    data_raw = pd.concat(data_raw)
                # elif option == 'np':
                #     data_raw = [reading_csv_dd(f'./data/5m/A{file}.csv',datetime_start,datetime_end) for file in code_stock]
                #     data_raw = np.concatenate(data_raw, axis=0)
                # elif option == 'pd-np':
                #     data_raw = [reading_csv(f'./data/5m/A{file}.csv', datetime_start, datetime_end) for file in
                #                code_stock]
                #     data_raw = pd.concat(data_raw)
                #     data_raw = data_raw.to_numpy()
            elif timeframe == 'minute1':
                data_raw = [reading_csv(f'./data/1m/A{file}.csv',datetime_start,datetime_end) for file in code_stock]
                data_raw = pd.concat(data_raw)

            self.data_raw = data_raw
            # self.data_raw = data_raw[
            #     (data_raw['date'] >= int((datetime_start - relativedelta(days=1)).strftime('%Y%m%d%H%M'))) & (
            #                 data_raw['date'] <= int(datetime_end.strftime('%Y%m%d%H%M')))]
        else:
            data_raw = reading_csv(f'./data/10m/A{code_stock}.csv',datetime_start,datetime_end)
            self.data_raw = data_raw
            # self.data_raw = data_raw[(data_raw['date']>=int((datetime_start-relativedelta(days=1)).strftime('%Y%m%d%H%M')))&(data_raw['date']<=int(datetime_end.strftime('%Y%m%d%H%M')))]
        self.trading_fee = 0.1
        self.data_dict = {}
        self.dict_run_time = {}

    def input_strategy(self, strategy_cls: type, strategy_list: None):
        self.strategy_list = strategy_list
        data_raw_temp = self.data_raw.copy()
        self.strategy: Strategy = strategy_cls(data_raw_temp, self.strategy_list)  # run_time, trading_fee 삭제

    def cal_strategy(self, name):
        run_time = datetime.now().strftime("%Y%m%d%H%M")
        self.data_dict[name] = self.strategy.calculate(run_time)  #여기서 run_time넣기.
        self.dict_run_time[name] = run_time


    # def cal_strategy(self, name, strategy_cls: type, strategy_list: None):
    #     self.strategy_list = strategy_list
    #     data_raw_temp = self.data_raw.copy()
    #     run_time = datetime.now().strftime("%Y%m%d%H%M")
    #     self.strategy: Strategy = strategy_cls(data_raw_temp, run_time, self.strategy_list, self.trading_fee)
    #     self.data_dict[name] = self.strategy.calculate()
    #     self.dict_run_time[name] = run_time

    def analyze_result_time1(self, name=None, max_profit=False):
        # name이 none일때는 전체 strategy에 대해서 출력되게 수정 필요
        data = self.data_dict[name]
        result = _analyze_result_time1(data, self.trading_fee, max_profit)
        with open(f'./result/{self.dict_run_time[name]}_result.pickle','wb') as handle:
            pickle.dump(result, handle, protocol=pickle.HIGHEST_PROTOCOL)
        return result

    def analyze_result_time(self, name=None):
        # name이 none일때는 전체 strategy에 대해서 출력되게 수정 필요
        data = self.data_dict[name]
        return _analyze_result_time(data)

    # 아직 미수정 (data_d가 없어짐)
    def analyze_result(self, add=False):
        if add:
            data_dict = self.data_dict
        else:
            data_dict = None
        return _analyze_result(self.data_d, data_dict)

    # 아직 미수정 (data_d가 없어짐)
    def plot_result(self):
        _plot_result(self.data_d)

    # 아직 미수정 (data_d가 없어짐)
    def corr_result(self):
        return self.data_d.corr()


def _plot_result(data):
    plt.figure(figsize=(10,6))
    for i in range(len(data.columns)):
        col_name = data.columns[i]
        plt.plot(data[col_name].cumsum())
    plt.legend(data.columns)
    plt.show()
def analyze_each_trade(trade, trading_fee, max_profit):
    if max_profit:
        return np.array([trade[0][0], trade[-1][0], trade[:,4].max()/trade[0][2]*(1-2*trading_fee*0.01/(1+trading_fee*0.01)) - 1, trade[0][1], trade[0][2], trade[-1][3]])
    else:
        return np.array([trade[0][0], trade[-1][0], trade[-1][3]/trade[0][2]*(1-2*trading_fee*0.01/(1+trading_fee*0.01)) - 1, trade[0][1], trade[0][2], trade[-1][3]])

def _analyze_result_time1(data, trading_fee, max_profit=False):
    array_trade_count = np.where((data[:, 10] == 1) & (np.roll(data[:, 10], 1) == 0), 1, 0).cumsum() * data[:,10]
    return np.r_[[analyze_each_trade(data[:, [0, 1, 6, 7, 3, 4]][array_trade_count == (i + 1)], trading_fee, max_profit) for i in
                  range(int(max(array_trade_count)) - 0)]]
# def analyze_each_trade(trade, max_profit):
#     if max_profit:
#         return np.array([trade[0][0], trade[-1][0], np.power(10, (trade[:, 2].cumsum()).max()) - 1, trade[0][1], trade[0][3], trade[-1][4]])
#     else:
#         return np.array([trade[0][0], trade[-1][0], np.power(10, trade[:, 2].sum()) - 1, trade[0][1], trade[0][3], trade[-1][4]])
#
# def _analyze_result_time1(data, max_profit=False):
#     array_trade_count = np.where((data[:, 8] == 1) & (np.roll(data[:, 8], 1) == 0), 1, 0).cumsum() * data[:,8]
#     return np.r_[[analyze_each_trade(data[:, [0, 1, 10, 4, 5]][array_trade_count == (i + 1)], max_profit) for i in
#                   range(int(max(array_trade_count)) - 1)]]

def _analyze_result_time(data):
    data['status_fill_temp'] = np.where((data.status_fill==1) & (data.status_fill.shift(1)==0), 1, 0).cumsum()
    data['status_fill_temp'] = data['status_fill'] * data['status_fill_temp']

    rtn = np.power(10, data[data.status_fill_temp != 0].groupby('status_fill_temp')['strategy_return'].agg('sum'))-1

    # data['date'] = data.index
    first_date = data[data.status_fill_temp != 0].groupby('status_fill_temp')['date'].nth(0)
    last_date = data[data.status_fill_temp != 0].groupby('status_fill_temp')['date'].nth(-1)
    code_stock = data[data.status_fill_temp != 0].groupby('status_fill_temp')['code_stock'].nth(0)
    df_temp = pd.DataFrame({'start':first_date, 'end':last_date, 'return':rtn, 'code':code_stock})
    df_temp.reset_index(inplace=True, drop=True)
    return df_temp

def _analyze_result_ind(data):
    name_list = ['no_trade', 'positive_ratio', 'avg_rlt', 'avg_gain', 'avg_loss']
    data['status_fill_temp'] = np.where((data.status_fill==1) & (data.status_fill.shift(1)==0), 1, 0).cumsum()
    data['status_fill_temp'] = data['status_fill'] * data['status_fill_temp']
    rslt = data[data.status_fill_temp != 0].groupby('status_fill_temp')['strategy_return'].agg(['count','sum'])
    array_trading_period = rslt['count']
    array_trading_profit = np.power(10,rslt['sum'])-1
    number_of_trading = len(array_trading_period)
    array_trading_profit_positive = array_trading_profit[array_trading_profit>0]
    array_trading_profit_negative = array_trading_profit[array_trading_profit<=0]
    number_of_trading_positive = len(array_trading_profit_positive)
    number_of_trading_negative = len(array_trading_profit_negative)
    hit_ratio = number_of_trading_positive / number_of_trading
    expactacy_per_trade = (array_trading_profit_positive.sum()+array_trading_profit_negative.sum())/number_of_trading
    average_gain = array_trading_profit_positive.mean()
    average_loss = array_trading_profit_negative.mean()
    largest_gain = array_trading_profit_positive.max()
    largest_loss = array_trading_profit_negative.min()
    ror = calculate_ror(hit_ratio, average_gain, average_loss)
    result_output = [number_of_trading , round(hit_ratio,2), round(expactacy_per_trade*100,2), round(average_gain*100,2), round(average_loss*100,2)]
    result_ind = pd.DataFrame([result_output], columns=name_list)
    return result_ind
def calculate_ror(win_rate, profit_ratio, loss_ratio, risked_capital = 0.2):
    z_value = win_rate*profit_ratio - (1-win_rate)*np.abs(loss_ratio)
    a_value = np.sqrt(win_rate*(profit_ratio**2) + (1-win_rate)*(loss_ratio**2))
    p_value = 0.5 * (1 + z_value/a_value)
    risk_of_ruin = ((1-p_value)/p_value)**(risked_capital / a_value)
    return risk_of_ruin

def _analyze_each_result(data_each, data_dict=None):
    name_list = ['name','start','end',"times_in_market", "avg_rtn", "total_profit", "cagr", "mdd", "sortino", 'worst_rtn','RoR']
    col_name = data_each.name
    time_start = data_each.index[0]
    time_end = data_each.index[-1]
    return_result = _power10(data_each)-1
    exp = qs.stats.exposure(return_result)
    ar = qs.stats.avg_return(return_result)
    arw = qs.stats.avg_win(return_result)
    arl = qs.stats.avg_loss(return_result)
    comp = qs.stats.comp(return_result)
    cagr = qs.stats.cagr(return_result)
    mdd = qs.stats.max_drawdown(return_result)
    srtn = qs.stats.sortino(return_result)
    rtn_record = qs.stats.monthly_returns(return_result)
    est_month_array = np.array([np.power(10, (np.random.choice(data_each, 30)).sum()) for _ in range(1000)])
    ror = sum(est_month_array<0.9)/1000
    result_output = [col_name, time_start.strftime("%y%m%d"),time_end.strftime("%y%m%d"), round(exp*100,2), f'{ar*100:.2f} / {arw*100:.2f} / {arl*100:.2f}',round(comp*100,2), round(cagr*100,3), round(mdd*100,2), round(srtn,2), sorted(np.round(rtn_record.iloc[:,:12].values.flatten()*100,2))[:5], ror]
    result_each = pd.DataFrame([result_output], columns=name_list)
    if data_dict is not None:
        if col_name in data_dict.keys():
            result_each = pd.concat([result_each, _analyze_result_ind(data_dict[col_name].loc[time_start:time_end])], axis=1)
    return result_each


def _analyze_result(data, data_dict=None):
    df_analysis = pd.DataFrame()
    for i in range(len(data.columns)):
        start_date = data.index[0]
        end_date = data.index[-1]
        if int(start_date.strftime("%y"))<22:
            if int(end_date.strftime("%y"))>=22:
                df_temp = _analyze_each_result(data.iloc[:,i], data_dict)
                df_analysis = pd.concat([df_analysis, df_temp], ignore_index=True)
                data1 = data.loc[:datetime(2022,1,1)]
                df_temp = _analyze_each_result(data1.iloc[:,i], data_dict)
                df_analysis = pd.concat([df_analysis, df_temp], ignore_index=True)
                data2 = data.loc[datetime(2022,1,1):]
                df_temp = _analyze_each_result(data2.iloc[:,i], data_dict)
                df_analysis = pd.concat([df_analysis, df_temp], ignore_index=True)
            else:
                df_temp = _analyze_each_result(data.iloc[:,i], data_dict)
                df_analysis = pd.concat([df_analysis, df_temp], ignore_index=True)
        else:
            df_temp = _analyze_each_result(data.iloc[:,i], data_dict)
            df_analysis = pd.concat([df_analysis, df_temp], ignore_index=True)
    return df_analysis

def define_data ( start , end , code, timeframe = 'minute10') :
    if timeframe == 'hourly':
        data_all = pd.read_csv(f'./data/hr/A{code}.csv',index_col=0)
    elif timeframe == 'minute10':
        # data_all = pd.read_csv(f'./data/10m/A{code}.csv',index_col=0)
        py = pc.read_csv(f'./data/10m/A{code}.csv')
        data_all = py.to_pandas().iloc[:, 1:]
        data_all['date'] = [datetime.strptime(str(i), '%Y%m%d%H%M') for i in data_all['date']]
    elif timeframe == 'minute1':
        data_all = pd.read_csv(f'./data/1m/A{code}.csv',index_col=0)
        data_all.rename(columns={'날짜': 'date', '시간': 'time', '시가': 'open', '고가': 'high', '저가': 'low', '종가': 'close',
                                 '거래량': 'volume'}, inplace=True)
        data_all['date'] = [datetime.strptime(str(d) + str(t).zfill(4), '%Y%m%d%H%M') for d, t in
                            zip(data_all.date, data_all.time)]
        data_all[['date', 'open', 'high', 'low', 'close', 'volume']]
    elif timeframe == 'daily':
        data_all = pd.read_csv(f'./data/day/A{code}.csv',index_col=0)
    else:
        print("Timeframe 확인")

    # data_all.rename(columns={'날짜':'date','시간':'time','시가':'open','고가':'high','저가':'low','종가':'close','거래량':'volume'},inplace=True)
    # data_all['date'] = [datetime.strptime(str(d)+str(t).zfill(4),'%Y%m%d%H%M')  for d, t in zip(data_all.date, data_all.time)]
    # data_all = data_all[['date','open','high','low','close','volume']]
    # data_all['date'] = [datetime.strptime(str(i),'%Y%m%d%H%M') for i in data_all['date']]
    data_all = data_all[(data_all.date >= start) & (data_all.date <= end)]
    data_all['return'] = np.log10(data_all.close / data_all.close.shift(1))
    #     data_all.reset_index(drop=True, inplace=True)
    data_all.index = data_all.date
    data_all.index.name = None
    data_all.sort_index(inplace=True)
#     data_all['open'] = data_all['close'].shift(1)
    return data_all[['open', 'high', 'low', 'close', 'volume']]