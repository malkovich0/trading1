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


class Backtest():
    def __init__(self, datetime_start, datetime_end, code_coin):
        self.data_hr = define_data(datetime_start - relativedelta(days=10), datetime_end, code_coin,
                                   timeframe='minute10')
        self.data_d = self.data_hr[['close']].resample('24H', origin=self.data_hr.index[0].replace(hour=9)).apply(
            {'close': 'last'})
        self.data_d['return'] = np.log10(self.data_d['close'] / self.data_d['close'].shift(1))
        self.data_d = self.data_d[['return']].loc[datetime_start:datetime_end]
        self.trading_fee = 0.1
        self.data_dict = {}

    def cal_strategy(self, name, strategy_cls: type, strategy_list: None):
        self.strategy_list = strategy_list
        data_d_temp = self.data_d.copy()
        data_hr_temp = self.data_hr.copy()
        self.strategy: Strategy = strategy_cls(data_d_temp, data_hr_temp, self.strategy_list, self.trading_fee)
        self.data_dict[name], self.data_d[name] = self.strategy.calculate()

    def analyze_result(self, add=False):
        if add:
            data_dict = self.data_dict
        else:
            data_dict = None
        return _analyze_result(self.data_d, data_dict)

    def plot_result(self):
        _plot_result(self.data_d)

    def corr_result(self):
        return self.data_d.corr()

def _plot_result(data):
    plt.figure(figsize=(10,6))
    for i in range(len(data.columns)):
        col_name = data.columns[i]
        plt.plot(data[col_name].cumsum())
    plt.legend(data.columns)
    plt.show()

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


def define_data ( start , end , code, timeframe = 'daily', time_start = 9 ) :
    if timeframe == 'daily':
        if time_start == 9:
            data_all = pd.read_pickle("./data/data_coin_daily.pkl")
            data_all = data_all[data_all.coin == code]
            data_all = data_all[(data_all.date >= start)&(data_all.date <= end)]
        else:
            data_hourly = pd.read_pickle('./data/data_coin_hourly.pkl')
            data_hourly = data_hourly[data_hourly.coin == code]
            start_datetime = start.replace(hour=time_start)
            data_hourly = data_hourly[data_hourly.date >= start_datetime]
            data_hourly.index = data_hourly.date
            dict_ohlcv = {'open':'first','high':'max','low':'min','close':'last','value':'sum','volume':'sum','date':'first'}
            data_all = data_hourly.resample('24H',origin=start_datetime).apply(dict_ohlcv)
            data_all = data_all[(data_all.date >= start)&(data_all.date <= end)]
    elif timeframe == 'hourly':
        data_all = pd.read_pickle('./data/data_coin_hourly.pkl')
        data_all = data_all[data_all.coin == code]
        data_all = data_all[(data_all.date >= start)&(data_all.date <= end)]
    elif timeframe == 'minute10':
        data_all = pd.read_pickle('./data/data_coin_minute10.pkl')
        data_all = data_all[data_all.coin == code]
        data_all = data_all[(data_all.date >= start)&(data_all.date <= end)]
    elif timeframe == 'minute1':
        data_all = pd.read_pickle('./data/data_coin_minute1.pkl')
        data_all = data_all[data_all.coin == code]
        data_all = data_all[(data_all.date >= start)&(data_all.date <= end)]
    else:
        print("Timeframe 확인")
    data_all['return'] = np.log10(data_all.close / data_all.close.shift(1))
#     data_all.reset_index(drop=True, inplace=True)
    data_all.index = data_all.date
    data_all.index.name = None
    data_all['open'] = data_all['close'].shift(1)
    return data_all[['open','high','low','close','volume','value']]