from module import upbit
import pandas as pd
import numpy as np

from decimal import Decimal


# -----------------------------------------------------------------------------
# - Name : get_rsi
# - Desc : RSI 조회
# - Input
#   1) target_item : 대상 종목
#   2) tick_kind : 캔들 종류 (1, 3, 5, 10, 15, 30, 60, 240 - 분, D-일, W-주, M-월)
#   3) inq_range : 조회 범위
# - Output
#   1) RSI 값
# -----------------------------------------------------------------------------
def get_rsi(target_item, tick_kind, inq_range):
    try:

        # 캔들 추출
        candle_data = upbit.get_candle(target_item, tick_kind, inq_range)

        df = pd.DataFrame(candle_data)
        df = df.reindex(index=df.index[::-1]).reset_index()

        df['close'] = df["trade_price"]

        # RSI 계산
        def rsi(ohlc: pd.DataFrame, period: int = 14):
            ohlc["close"] = ohlc["close"]
            delta = ohlc["close"].diff()

            up, down = delta.copy(), delta.copy()
            up[up < 0] = 0
            down[down > 0] = 0

            _gain = up.ewm(com=(period - 1), min_periods=period).mean()
            _loss = down.abs().ewm(com=(period - 1), min_periods=period).mean()

            RS = _gain / _loss
            return pd.Series(100 - (100 / (1 + RS)), name="RSI")

        rsi = round(rsi(df, 14).iloc[-1], 4)

        return rsi


    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : get_mfi
# - Desc : MFI 조회
# - Input
#   1) target_item : 대상 종목
#   2) tick_kind : 캔들 종류 (1, 3, 5, 10, 15, 30, 60, 240 - 분, D-일, W-주, M-월)
#   3) inq_range : 캔들 조회 범위
#   4) loop_cnt : 지표 반복계산 횟수
# - Output
#   1) MFI 값
# -----------------------------------------------------------------------------
def get_mfi(target_item, tick_kind, inq_range, loop_cnt):
    try:

        # 캔들 데이터 조회용
        candle_datas = []

        # MFI 데이터 리턴용
        mfi_list = []

        # 캔들 추출
        candle_data = upbit.get_candle(target_item, tick_kind, inq_range)

        # 조회 횟수별 candle 데이터 조합
        for i in range(0, int(loop_cnt)):
            candle_datas.append(candle_data[i:int(len(candle_data))])

        # 캔들 데이터만큼 수행
        for candle_data_for in candle_datas:

            df = pd.DataFrame(candle_data_for)
            dfDt = df['candle_date_time_kst'].iloc[::-1]

            df['typical_price'] = (df['trade_price'] + df['high_price'] + df['low_price']) / 3
            df['money_flow'] = df['typical_price'] * df['candle_acc_trade_volume']

            positive_mf = 0
            negative_mf = 0

            for i in range(0, 14):

                if df["typical_price"][i] > df["typical_price"][i + 1]:
                    positive_mf = positive_mf + df["money_flow"][i]
                elif df["typical_price"][i] < df["typical_price"][i + 1]:
                    negative_mf = negative_mf + df["money_flow"][i]

            if negative_mf > 0:
                mfi = 100 - (100 / (1 + (positive_mf / negative_mf)))
            else:
                mfi = 100 - (100 / (1 + (positive_mf)))

            mfi_list.append({"type": "MFI", "DT": dfDt[0], "MFI": round(mfi, 4)})

        return mfi_list

    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : get_macd
# - Desc : MACD 조회
# - Input
#   1) target_item : 대상 종목
#   2) tick_kind : 캔들 종류 (1, 3, 5, 10, 15, 30, 60, 240 - 분, D-일, W-주, M-월)
#   3) inq_range : 캔들 조회 범위
#   4) loop_cnt : 지표 반복계산 횟수
# - Output
#   1) MACD 값
# -----------------------------------------------------------------------------
def get_macd(target_item, tick_kind, inq_range, loop_cnt):
    try:

        # 캔들 데이터 조회용
        candle_datas = []

        # MACD 데이터 리턴용
        macd_list = []

        # 캔들 추출
        candle_data = upbit.get_candle(target_item, tick_kind, inq_range)

        # 조회 횟수별 candle 데이터 조합
        for i in range(0, int(loop_cnt)):
            candle_datas.append(candle_data[i:int(len(candle_data))])

        df = pd.DataFrame(candle_datas[0])
        df = df.iloc[::-1]
        df = df['trade_price']

        # MACD 계산
        exp1 = df.ewm(span=12, adjust=False).mean()
        exp2 = df.ewm(span=26, adjust=False).mean()
        macd = exp1 - exp2
        exp3 = macd.ewm(span=9, adjust=False).mean()

        for i in range(0, int(loop_cnt)):
            macd_list.append(
                {"type": "MACD", "DT": candle_datas[0][i]['candle_date_time_kst'], "MACD": round(macd[i], 4),
                 "SIGNAL": round(exp3[i], 4),
                 "OCL": round(macd[i] - exp3[i], 4)})

        return macd_list

    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : get_bb
# - Desc : 볼린저밴드 조회
# - Input
#   1) target_item : 대상 종목
#   2) tick_kind : 캔들 종류 (1, 3, 5, 10, 15, 30, 60, 240 - 분, D-일, W-주, M-월)
#   3) inq_range : 캔들 조회 범위
#   4) loop_cnt : 지표 반복계산 횟수
# - Output
#   1) 볼린저 밴드 값
# -----------------------------------------------------------------------------
def get_bb(target_item, tick_kind, inq_range, loop_cnt):
    try:

        # 캔들 데이터 조회용
        candle_datas = []

        # 볼린저밴드 데이터 리턴용
        bb_list = []

        # 캔들 추출
        candle_data = upbit.get_candle(target_item, tick_kind, inq_range)

        # 조회 횟수별 candle 데이터 조합
        for i in range(0, int(loop_cnt)):
            candle_datas.append(candle_data[i:int(len(candle_data))])

        # 캔들 데이터만큼 수행
        for candle_data_for in candle_datas:
            df = pd.DataFrame(candle_data_for)
            dfDt = df['candle_date_time_kst'].iloc[::-1]
            df = df['trade_price'].iloc[::-1]

            # 표준편차(곱)
            unit = 2

            band1 = unit * np.std(df[len(df) - 20:len(df)])
            bb_center = np.mean(df[len(df) - 20:len(df)])
            band_high = bb_center + band1
            band_low = bb_center - band1

            bb_list.append({"type": "BB", "DT": dfDt[0], "BBH": round(band_high, 4), "BBM": round(bb_center, 4),
                            "BBL": round(band_low, 4)})

        return bb_list


    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : get_williams
# - Desc : 윌리암스 %R 조회
# - Input
#   1) target_item : 대상 종목
#   2) tick_kind : 캔들 종류 (1, 3, 5, 10, 15, 30, 60, 240 - 분, D-일, W-주, M-월)
#   3) inq_range : 캔들 조회 범위
#   4) loop_cnt : 지표 반복계산 횟수
# - Output
#   1) 윌리암스 %R 값
# -----------------------------------------------------------------------------
def get_williamsR(target_item, tick_kind, inq_range, loop_cnt):
    try:

        # 캔들 데이터 조회용
        candle_datas = []

        # 윌리암스R 데이터 리턴용
        williams_list = []

        # 캔들 추출
        candle_data = upbit.get_candle(target_item, tick_kind, inq_range)

        # 조회 횟수별 candle 데이터 조합
        for i in range(0, int(loop_cnt)):
            candle_datas.append(candle_data[i:int(len(candle_data))])

        # 캔들 데이터만큼 수행
        for candle_data_for in candle_datas:
            df = pd.DataFrame(candle_data_for)
            dfDt = df['candle_date_time_kst'].iloc[::-1]
            df = df.iloc[:14]

            # 계산식
            # %R = (Highest High - Close)/(Highest High - Lowest Low) * -100
            hh = np.max(df['high_price'])
            ll = np.min(df['low_price'])
            cp = df['trade_price'][0]

            w = (hh - cp) / (hh - ll) * -100

            williams_list.append(
                {"type": "WILLIAMS", "DT": dfDt[0], "HH": round(hh, 4), "LL": round(ll, 4), "CP": round(cp, 4),
                 "W": round(w, 4)})

        return williams_list


    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : get_rsi
# - Desc : RSI 조회
# - Input
#   1) candle_data : 캔들 정보
# - Output
#   1) RSI 값
# -----------------------------------------------------------------------------
def get_rsi(candle_datas):
    try:

        # RSI 데이터 리턴용
        rsi_data = []

        # 캔들 데이터만큼 수행
        for candle_data_for in candle_datas:
            df = pd.DataFrame(candle_data_for)
            dfDt = df['candle_date_time_kst'].iloc[::-1]
            df = df.reindex(index=df.index[::-1]).reset_index()

            df['close'] = df["trade_price"]

            # RSI 계산
            def rsi(ohlc: pd.DataFrame, period: int = 14):
                ohlc["close"] = ohlc["close"]
                delta = ohlc["close"].diff()

                up, down = delta.copy(), delta.copy()
                up[up < 0] = 0
                down[down > 0] = 0

                _gain = up.ewm(com=(period - 1), min_periods=period).mean()
                _loss = down.abs().ewm(com=(period - 1), min_periods=period).mean()

                RS = _gain / _loss
                return pd.Series(100 - (100 / (1 + RS)), name="RSI")

            rsi = round(rsi(df, 14).iloc[-1], 4)
            rsi_data.append({"type": "RSI", "DT": dfDt[0], "RSI": rsi})

        return rsi_data

    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : get_mfi
# - Desc : MFI 조회
# - Input
#   1) candle_datas : 캔들 정보
# - Output
#   1) MFI 값
# -----------------------------------------------------------------------------
def get_mfi(candle_datas):
    try:

        # MFI 데이터 리턴용
        mfi_list = []

        # 캔들 데이터만큼 수행
        for candle_data_for in candle_datas:

            df = pd.DataFrame(candle_data_for)
            dfDt = df['candle_date_time_kst'].iloc[::-1]

            df['typical_price'] = (df['trade_price'] + df['high_price'] + df['low_price']) / 3
            df['money_flow'] = df['typical_price'] * df['candle_acc_trade_volume']

            positive_mf = 0
            negative_mf = 0

            for i in range(0, 14):

                if df["typical_price"][i] > df["typical_price"][i + 1]:
                    positive_mf = positive_mf + df["money_flow"][i]
                elif df["typical_price"][i] < df["typical_price"][i + 1]:
                    negative_mf = negative_mf + df["money_flow"][i]

            if negative_mf > 0:
                mfi = 100 - (100 / (1 + (positive_mf / negative_mf)))
            else:
                mfi = 100 - (100 / (1 + (positive_mf)))

            mfi_list.append({"type": "MFI", "DT": dfDt[0], "MFI": round(mfi, 4)})

        return mfi_list

    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : get_macd
# - Desc : MACD 조회
# - Input
#   1) candle_datas : 캔들 정보
#   2) loop_cnt : 반복 횟수
# - Output
#   1) MACD 값
# -----------------------------------------------------------------------------
def get_macd(candle_datas, loop_cnt):
    try:

        # MACD 데이터 리턴용
        macd_list = []

        df = pd.DataFrame(candle_datas[0])
        df = df.iloc[::-1]
        df = df['trade_price']

        # MACD 계산
        exp1 = df.ewm(span=12, adjust=False).mean()
        exp2 = df.ewm(span=26, adjust=False).mean()
        macd = exp1 - exp2
        exp3 = macd.ewm(span=9, adjust=False).mean()

        for i in range(0, int(loop_cnt)):
            macd_list.append(
                {"type": "MACD", "DT": candle_datas[0][i]['candle_date_time_kst'], "MACD": round(macd[i], 4),
                 "SIGNAL": round(exp3[i], 4),
                 "OCL": round(macd[i] - exp3[i], 4)})

        return macd_list

    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : get_ma
# - Desc : MA 조회
# - Input
#   1) candle_datas : 캔들 정보
#   2) loop_cnt : 반복 횟수
# - Output
#   1) MA 값
# -----------------------------------------------------------------------------
def get_ma(candle_datas, loop_cnt):
    try:
        # MA 데이터 리턴용
        ma_list = []

        df = pd.DataFrame(candle_datas[0])
        df = df.iloc[::-1]
        df = df['trade_price']

        # MA 계산

        ma5 = df.rolling(window=5).mean()
        ma10 = df.rolling(window=10).mean()
        ma20 = df.rolling(window=20).mean()
        ma60 = df.rolling(window=60).mean()
        ma120 = df.rolling(window=120).mean()

        for i in range(0, int(loop_cnt)):
            ma_list.append(
                {"type": "MA", "DT": candle_datas[0][i]['candle_date_time_kst'], "MA5": ma5[i], "MA10": ma10[i],
                 "MA20": ma20[i], "MA60": ma60[i], "MA120": ma120[i]
                    , "MA_5_10": str(Decimal(str(ma5[i])) - Decimal(str(ma10[i])))
                    , "MA_10_20": str(Decimal(str(ma10[i])) - Decimal(str(ma20[i])))
                    , "MA_20_60": str(Decimal(str(ma20[i])) - Decimal(str(ma60[i])))
                    , "MA_60_120": str(Decimal(str(ma60[i])) - Decimal(str(ma120[i])))})

        return ma_list

    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : get_bb
# - Desc : 볼린저밴드 조회
# - Input
#   1) candle_datas : 캔들 정보
# - Output
#   1) 볼린저 밴드 값
# -----------------------------------------------------------------------------
def get_bb(candle_datas):
    try:

        # 볼린저밴드 데이터 리턴용
        bb_list = []

        # 캔들 데이터만큼 수행
        for candle_data_for in candle_datas:
            df = pd.DataFrame(candle_data_for)
            dfDt = df['candle_date_time_kst'].iloc[::-1]
            df = df['trade_price'].iloc[::-1]

            # 표준편차(곱)
            unit = 2

            band1 = unit * np.std(df[len(df) - 20:len(df)])
            bb_center = np.mean(df[len(df) - 20:len(df)])
            band_high = bb_center + band1
            band_low = bb_center - band1

            bb_list.append({"type": "BB", "DT": dfDt[0], "BBH": round(band_high, 4), "BBM": round(bb_center, 4),
                            "BBL": round(band_low, 4)})

        return bb_list


    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : get_williams
# - Desc : 윌리암스 %R 조회
# - Input
#   1) candle_datas : 캔들 정보
# - Output
#   1) 윌리암스 %R 값
# -----------------------------------------------------------------------------
def get_williams(candle_datas):
    try:

        # 윌리암스R 데이터 리턴용
        williams_list = []

        # 캔들 데이터만큼 수행
        for candle_data_for in candle_datas:
            df = pd.DataFrame(candle_data_for)
            dfDt = df['candle_date_time_kst'].iloc[::-1]
            df = df.iloc[:14]

            # 계산식
            # %R = (Highest High - Close)/(Highest High - Lowest Low) * -100
            hh = np.max(df['high_price'])
            ll = np.min(df['low_price'])
            cp = df['trade_price'][0]

            w = (hh - cp) / (hh - ll) * -100

            williams_list.append(
                {"type": "WILLIAMS", "DT": dfDt[0], "HH": round(hh, 4), "LL": round(ll, 4), "CP": round(cp, 4),
                 "W": round(w, 4)})

        return williams_list


    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : get_cci
# - Desc : CCI 조회
# - Input
#   1) candle_data : 캔들 정보
#   2) loop_cnt : 조회 건수
# - Output
#   1) CCI 값
# -----------------------------------------------------------------------------
def get_cci(candle_data, loop_cnt):
    try:

        cci_val = 20

        # CCI 데이터 리턴용
        cci_list = []

        # 사용하지 않는 캔들 갯수 정리(속도 개선)
        del candle_data[cci_val * 2:]

        # 오름차순 정렬
        df = pd.DataFrame(candle_data)
        ordered_df = df.sort_values(by=['candle_date_time_kst'], ascending=[True])

        # 계산식 : (Typical Price - Simple Moving Average) / (0.015 * Mean absolute Deviation)
        ordered_df['TP'] = (ordered_df['high_price'] + ordered_df['low_price'] + ordered_df['trade_price']) / 3
        ordered_df['SMA'] = ordered_df['TP'].rolling(window=cci_val).mean()
        ordered_df['MAD'] = ordered_df['TP'].rolling(window=cci_val).apply(lambda x: pd.Series(x).mad())
        ordered_df['CCI'] = (ordered_df['TP'] - ordered_df['SMA']) / (0.015 * ordered_df['MAD'])

        # 개수만큼 조립
        for i in range(0, loop_cnt):
            cci_list.append({"type": "CCI", "DT": ordered_df['candle_date_time_kst'].loc[i],
                             "CCI": round(ordered_df['CCI'].loc[i], 4)})

        return cci_list

    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : get_indicators
# - Desc : 보조지표 조회
# - Input
#   1) target_item : 대상 종목
#   2) tick_kind : 캔들 종류 (1, 3, 5, 10, 15, 30, 60, 240 - 분, D-일, W-주, M-월)
#   3) inq_range : 캔들 조회 범위
#   4) loop_cnt : 지표 반복계산 횟수
# - Output
#   1) RSI
#   2) MFI
#   3) MACD
#   4) BB
#   5) WILLIAMS %R
#   6) CCI
# -----------------------------------------------------------------------------
def get_indicators(target_item, tick_kind, inq_range, loop_cnt):
    try:

        # 보조지표 리턴용
        indicator_data = []

        # 캔들 데이터 조회용
        candle_datas = []

        # 캔들 추출
        candle_data = upbit.get_candle(target_item, tick_kind, inq_range)

        if len(candle_data) >= 30:

            # 조회 횟수별 candle 데이터 조합
            for i in range(0, int(loop_cnt)):
                candle_datas.append(candle_data[i:int(len(candle_data))])

            # RSI 정보 조회
            rsi_data = get_rsi(candle_datas)

            # MFI 정보 조회
            mfi_data = get_mfi(candle_datas)

            # MACD 정보 조회
            macd_data = get_macd(candle_datas, loop_cnt)

            # BB 정보 조회
            bb_data = get_bb(candle_datas)

            # WILLIAMS %R 조회
            williams_data = get_williams(candle_datas)

            # MA 정보 조회
            ma_data = get_ma(candle_datas, loop_cnt)

            # CCI 정보 조회
            cci_data = get_cci(candle_data, loop_cnt)

            if len(rsi_data) > 0:
                indicator_data.append(rsi_data)

            if len(mfi_data) > 0:
                indicator_data.append(mfi_data)

            if len(macd_data) > 0:
                indicator_data.append(macd_data)

            if len(bb_data) > 0:
                indicator_data.append(bb_data)

            if len(williams_data) > 0:
                indicator_data.append(williams_data)

            if len(ma_data) > 0:
                indicator_data.append(ma_data)

            if len(cci_data) > 0:
                indicator_data.append(cci_data)

        return indicator_data

    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception:
        raise

# -----------------------------------------------------------------------------
# - Name : get_indicator_sel
# - Desc : 보조지표 조회(원하는 지표만)
# - Input
#   1) target_item : 대상 종목
#   2) tick_kind : 캔들 종류 (1, 3, 5, 10, 15, 30, 60, 240 - 분, D-일, W-주, M-월)
#   3) inq_range : 캔들 조회 범위
#   4) loop_cnt : 지표 반복계산 횟수
#   5) 보조지표 : 리스트
# - Output
#   1) 보조지표
# -----------------------------------------------------------------------------
def get_indicator_sel(target_item, tick_kind, inq_range, loop_cnt, indi_type):
    try:

        # 보조지표 리턴용
        indicator_data = {}

        # 캔들 데이터 조회용
        candle_datas = []

        # 캔들 추출
        candle_data = upbit.get_candle(target_item, tick_kind, inq_range)

        if len(candle_data) >= 30:

            # 조회 횟수별 candle 데이터 조합
            for i in range(0, int(loop_cnt)):
                candle_datas.append(candle_data[i:int(len(candle_data))])

            if 'RSI' in indi_type:
                # RSI 정보 조회
                rsi_data = get_rsi(candle_datas)
                indicator_data['RSI'] = rsi_data

            if 'MFI' in indi_type:
                # MFI 정보 조회
                mfi_data = get_mfi(candle_datas)
                indicator_data['MFI'] = mfi_data

            if 'MACD' in indi_type:
                # MACD 정보 조회
                macd_data = get_macd(candle_datas, loop_cnt)
                indicator_data['MACD'] = macd_data

            if 'BB' in indi_type:
                # BB 정보 조회
                bb_data = get_bb(candle_datas)
                indicator_data['BB'] = bb_data

            if 'WILLIAMS' in indi_type:
                # WILLIAMS %R 조회
                williams_data = get_williams(candle_datas)
                indicator_data['WILLIAMS'] = williams_data

            if 'MA' in indi_type:
                # MA 정보 조회
                ma_data = get_ma(candle_datas, loop_cnt)
                indicator_data['MA'] = ma_data

            if 'CCI' in indi_type:
                # CCI 정보 조회
                cci_data = get_cci(candle_data, loop_cnt)
                indicator_data['CCI'] = cci_data

            if 'CANDLE' in indi_type:
                indicator_data['CANDLE'] = candle_data

        return indicator_data

    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception:
        raise
