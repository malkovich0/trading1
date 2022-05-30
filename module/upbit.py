import time
import logging
import requests
import jwt
import uuid
import hashlib
import math
import os
import pandas as pd
import numpy as np

from urllib.parse import urlencode
from decimal import Decimal
from datetime import datetime

# Keys
import pickle
with open('./info/login_info.pickle','rb') as handle:
    login_info = pickle.load(handle)
access_key = login_info['access_key']
secret_key = login_info['secret_key']
server_url = login_info['server_url']
line_target_url = login_info['line_target_url']
line_token = login_info['line_token']

# 상수 설정
min_order_amt = 5000


# -----------------------------------------------------------------------------
# - Name : set_loglevel
# - Desc : 로그레벨 설정
# - Input
#   1) level : 로그레벨
#     1. D(d) : DEBUG
#     2. E(e) : ERROR
#     3. 그외(기본) : INFO
# - Output
# -----------------------------------------------------------------------------
def set_loglevel(level):
    try:

        # ---------------------------------------------------------------------
        # 로그레벨 : DEBUG
        # ---------------------------------------------------------------------
        if level.upper() == "D":
            logging.basicConfig(
                format='[%(asctime)s][%(levelname)s][%(filename)s:%(lineno)d]:%(message)s',
                datefmt='%Y/%m/%d %I:%M:%S %p',
                level=logging.DEBUG
            )
        # ---------------------------------------------------------------------
        # 로그레벨 : ERROR
        # ---------------------------------------------------------------------
        elif level.upper() == "E":
            logging.basicConfig(
                format='[%(asctime)s][%(levelname)s][%(filename)s:%(lineno)d]:%(message)s',
                datefmt='%Y/%m/%d %I:%M:%S %p',
                level=logging.ERROR
            )
        # ---------------------------------------------------------------------
        # 로그레벨 : INFO
        # ---------------------------------------------------------------------
        else:
            # -----------------------------------------------------------------------------
            # 로깅 설정
            # 로그레벨(DEBUG, INFO, WARNING, ERROR, CRITICAL)
            # -----------------------------------------------------------------------------
            logging.basicConfig(
                format='[%(asctime)s][%(levelname)s][%(filename)s:%(lineno)d]:%(message)s',
                datefmt='%Y/%m/%d %I:%M:%S %p',
                level=logging.INFO
            )

    # ----------------------------------------
    # Exception Raise
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : send_request
# - Desc : 리퀘스트 처리
# - Input
#   1) reqType : 요청 타입
#   2) reqUrl : 요청 URL
#   3) reqParam : 요청 파라메타
#   4) reqHeader : 요청 헤더
# - Output
#   4) reponse : 응답 데이터
# -----------------------------------------------------------------------------
def send_request(reqType, reqUrl, reqParam, reqHeader):
    try:

        # 요청 가능회수 확보를 위해 기다리는 시간(초)
        err_sleep_time = 0.3

        # 요청에 대한 응답을 받을 때까지 반복 수행
        while True:

            # 요청 처리
            response = requests.request(reqType, reqUrl, params=reqParam, headers=reqHeader)

            # 요청 가능회수 추출
            if 'Remaining-Req' in response.headers:

                hearder_info = response.headers['Remaining-Req']
                start_idx = hearder_info.find("sec=")
                end_idx = len(hearder_info)
                remain_sec = hearder_info[int(start_idx):int(end_idx)].replace('sec=', '')
            else:
                logging.error("헤더 정보 이상")
                logging.error(response.headers)
                break

            # 요청 가능회수가 3개 미만이면 요청 가능회수 확보를 위해 일정시간 대기
            if int(remain_sec) < 3:
                logging.debug("요청 가능회수 한도 도달! 남은횟수:" + str(remain_sec))
                time.sleep(err_sleep_time)

            # 정상 응답
            if response.status_code == 200 or response.status_code == 201:
                break
            # 요청 가능회수 초과인 경우
            elif response.status_code == 429:
                logging.error("요청 가능회수 초과!:" + str(response.status_code))
                time.sleep(err_sleep_time)
            # 그 외 오류
            else:
                logging.error("기타 에러:" + str(response.status_code))
                logging.error(response.status_code)
                logging.error(response)
                break

            # 요청 가능회수 초과 에러 발생시에는 다시 요청
            logging.info("[restRequest] 요청 재처리중...")

        return response

    # ----------------------------------------
    # Exception Raise
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : get_items
# - Desc : 전체 종목 리스트 조회
# - Input
#   1) market : 대상 마켓(콤마 구분자:KRW,BTC,USDT)
#   2) except_item : 제외 종목(콤마 구분자:BTC,ETH)
# - Output
#   1) 전체 리스트 : 리스트
# -----------------------------------------------------------------------------
def get_items(market, except_item):
    try:

        # 조회결과 리턴용
        rtn_list = []

        # 마켓 데이터
        markets = market.split(',')

        # 제외 데이터
        except_items = except_item.split(',')

        url = "https://api.upbit.com/v1/market/all"
        querystring = {"isDetails": "false"}
        response = send_request("GET", url, querystring, "")
        data = response.json()

        # 조회 마켓만 추출
        for data_for in data:
            for market_for in markets:
                if data_for['market'].split('-')[0] == market_for:
                    rtn_list.append(data_for)

        # 제외 종목 제거
        for rtnlist_for in rtn_list[:]:
            for exceptItemFor in except_items:
                for marketFor in markets:
                    if rtnlist_for['market'] == marketFor + '-' + exceptItemFor:
                        rtn_list.remove(rtnlist_for)

        return rtn_list

    # ----------------------------------------
    # Exception Raise
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : buycoin_mp
# - Desc : 시장가 매수
# - Input
#   1) target_item : 대상종목
#   2) buy_amount : 매수금액
# - Output
#   1) rtn_data : 매수결과
# -----------------------------------------------------------------------------
def buycoin_mp(target_item, buy_amount):
    try:

        query = {
            'market': target_item,
            'side': 'bid',
            'price': buy_amount,
            'ord_type': 'price',
        }

        query_string = urlencode(query).encode()

        m = hashlib.sha512()
        m.update(query_string)
        query_hash = m.hexdigest()

        payload = {
            'access_key': access_key,
            'nonce': str(uuid.uuid4()),
            'query_hash': query_hash,
            'query_hash_alg': 'SHA512',
        }

        jwt_token = jwt.encode(payload, secret_key)
        authorize_token = 'Bearer {}'.format(jwt_token)
        headers = {"Authorization": authorize_token}

        res = send_request("POST", server_url + "/v1/orders", query, headers)
        rtn_data = res.json()

        logging.info("")
        logging.info("----------------------------------------------")
        logging.info("시장가 매수 요청 완료! 결과:")
        logging.info(rtn_data)
        logging.info("----------------------------------------------")

        return rtn_data

    # ----------------------------------------
    # Exception Raise
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : buycoin_tg
# - Desc : 지정가 매수
# - Input
#   1) target_item : 대상종목
#   2) buy_amount : 매수금액
#   3) buy_price : 매수가격
# - Output
#   1) rtn_data : 매수요청결과
# -----------------------------------------------------------------------------
def buycoin_tg(target_item, buy_amount, buy_price):
    try:

        # 매수수량 계산
        vol = Decimal(str(buy_amount)) / Decimal(str(buy_price))

        query = {
            'market': target_item,
            'side': 'bid',
            'volume': vol,
            'price': buy_price,
            'ord_type': 'limit',
        }

        query_string = urlencode(query).encode()

        m = hashlib.sha512()
        m.update(query_string)
        query_hash = m.hexdigest()

        payload = {
            'access_key': access_key,
            'nonce': str(uuid.uuid4()),
            'query_hash': query_hash,
            'query_hash_alg': 'SHA512',
        }

        jwt_token = jwt.encode(payload, secret_key)
        authorize_token = 'Bearer {}'.format(jwt_token)
        headers = {"Authorization": authorize_token}

        res = send_request("POST", server_url + "/v1/orders", query, headers)
        rtn_data = res.json()

        logging.info("")
        logging.info("----------------------------------------------")
        logging.info("지정가 매수요청 완료!")
        logging.info(rtn_data)
        logging.info("----------------------------------------------")

        return rtn_data

    # ----------------------------------------
    # Exception Raise
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : sellcoin_mp
# - Desc : 시장가 매도
# - Input
#   1) target_item : 대상종목
#   2) cancel_yn : 기존 주문 취소 여부
# - Output
#   1) rtn_data : 매도결과
# -----------------------------------------------------------------------------
# 시장가 매도
def sellcoin_mp(target_item, cancel_yn):
    try:

        if cancel_yn == 'Y':
            # 기존 주문이 있으면 취소
            cancel_order(target_item, "SELL")

        # 잔고 조회
        cur_balance = get_balance(target_item)

        query = {
            'market': target_item,
            'side': 'ask',
            'volume': cur_balance,
            'ord_type': 'market',
        }

        query_string = urlencode(query).encode()

        m = hashlib.sha512()
        m.update(query_string)
        query_hash = m.hexdigest()

        payload = {
            'access_key': access_key,
            'nonce': str(uuid.uuid4()),
            'query_hash': query_hash,
            'query_hash_alg': 'SHA512',
        }

        jwt_token = jwt.encode(payload, secret_key)
        authorize_token = 'Bearer {}'.format(jwt_token)
        headers = {"Authorization": authorize_token}

        res = send_request("POST", server_url + "/v1/orders", query, headers)
        rtn_data = res.json()

        logging.info("")
        logging.info("----------------------------------------------")
        logging.info("시장가 매도 요청 완료! 결과:")
        logging.info(rtn_data)
        logging.info("----------------------------------------------")

        return rtn_data

    # ----------------------------------------
    # Exception Raise
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : sellcoin_tg
# - Desc : 지정가 매도
# - Input
#   1) target_item : 대상종목
#   2) sell_price : 매도희망금액
# - Output
#   1) rtn_data : 매도결과
# -----------------------------------------------------------------------------
def sellcoin_tg(target_item, sell_price):
    try:

        # 잔고 조회
        cur_balance = get_balance(target_item)

        query = {
            'market': target_item,
            'side': 'ask',
            'volume': cur_balance,
            'price': sell_price,
            'ord_type': 'limit',
        }

        query_string = urlencode(query).encode()

        m = hashlib.sha512()
        m.update(query_string)
        query_hash = m.hexdigest()

        payload = {
            'access_key': access_key,
            'nonce': str(uuid.uuid4()),
            'query_hash': query_hash,
            'query_hash_alg': 'SHA512',
        }

        jwt_token = jwt.encode(payload, secret_key)
        authorize_token = 'Bearer {}'.format(jwt_token)
        headers = {"Authorization": authorize_token}

        res = send_request("POST", server_url + "/v1/orders", query, headers)
        rtn_data = res.json()

        logging.info("")
        logging.info("----------------------------------------------")
        logging.info("지정가 매도 설정 완료!")
        logging.info(rtn_data)
        logging.info("----------------------------------------------")

        return rtn_data

    # ----------------------------------------
    # Exception Raise
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : get_balance
# - Desc : 주문가능 잔고 조회
# - Input
#   1) target_item : 대상 종목
# - Output
#   2) rtn_balance : 주문가능 잔고
# -----------------------------------------------------------------------------
def get_balance(target_item):
    try:

        # 주문가능 잔고 리턴용
        rtn_balance = 0

        # 최대 재시도 횟수
        max_cnt = 0

        # 잔고가 조회 될 때까지 반복
        while True:

            # 조회 회수 증가
            max_cnt = max_cnt + 1

            payload = {
                'access_key': access_key,
                'nonce': str(uuid.uuid4()),
            }

            jwt_token = jwt.encode(payload, secret_key)
            authorize_token = 'Bearer {}'.format(jwt_token)
            headers = {"Authorization": authorize_token}

            res = send_request("GET", server_url + "/v1/accounts", "", headers)
            my_asset = res.json()

            # 해당 종목에 대한 잔고 조회
            # 잔고는 마켓에 상관없이 전체 잔고가 조회됨
            for myasset_for in my_asset:
                if myasset_for['currency'] == target_item.split('-')[1]:
                    rtn_balance = myasset_for['balance']

            # 잔고가 0 이상일때까지 반복
            if Decimal(str(rtn_balance)) > Decimal(str(0)):
                break

            # 최대 100회 수행
            if max_cnt > 100:
                break

            logging.info("[주문가능 잔고 리턴용] 요청 재처리중...")

        return rtn_balance

    # ----------------------------------------
    # Exception Raise
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : get_candle
# - Desc : 캔들 조회
# - Input
#   1) target_item : 대상 종목
#   2) tick_kind : 캔들 종류 (1, 3, 5, 10, 15, 30, 60, 240 - 분, D-일, W-주, M-월)
#   3) inq_range : 조회 범위
# - Output
#   1) 캔들 정보 배열
# -----------------------------------------------------------------------------
def get_candle(target_item, tick_kind, inq_range):
    try:

        # ----------------------------------------
        # Tick 별 호출 URL 설정
        # ----------------------------------------
        # 분붕
        if tick_kind == "1" or tick_kind == "3" or tick_kind == "5" or tick_kind == "10" or tick_kind == "15" or tick_kind == "30" or tick_kind == "60" or tick_kind == "240":
            target_url = "minutes/" + tick_kind
        # 일봉
        elif tick_kind == "D":
            target_url = "days"
        # 주봉
        elif tick_kind == "W":
            target_url = "weeks"
        # 월봉
        elif tick_kind == "M":
            target_url = "months"
        # 잘못된 입력
        else:
            raise Exception("잘못된 틱 종류:" + str(tick_kind))

        logging.debug(target_url)

        # ----------------------------------------
        # Tick 조회
        # ----------------------------------------
        querystring = {"market": target_item, "count": inq_range}
        res = send_request("GET", server_url + "/v1/candles/" + target_url, querystring, "")
        candle_data = res.json()

        logging.debug(candle_data)

        return candle_data

    # ----------------------------------------
    # Exception Raise
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : get_targetprice
# - Desc : 호가단위 금액 계산
# - Input
#   1) cal_type : H:호가로, R:비율로
#   2) st_price : 기준가격
#   3) chg_val : 변화단위
# - Output
#   1) rtn_price : 계산된 금액
# -----------------------------------------------------------------------------
def get_targetprice(cal_type, st_price, chg_val):
    try:
        # 계산된 가격
        rtn_price = st_price

        # 호가단위로 계산
        if cal_type.upper() == "H":

            for i in range(0, abs(int(chg_val))):

                hoga_val = get_hoga(rtn_price)

                if Decimal(str(chg_val)) > 0:
                    rtn_price = Decimal(str(rtn_price)) + Decimal(str(hoga_val))
                elif Decimal(str(chg_val)) < 0:
                    rtn_price = Decimal(str(rtn_price)) - Decimal(str(hoga_val))
                else:
                    break

        # 비율로 계산
        elif cal_type.upper() == "R":

            while True:

                # 호가단위 추출
                hoga_val = get_hoga(st_price)

                if Decimal(str(chg_val)) > 0:
                    rtn_price = Decimal(str(rtn_price)) + Decimal(str(hoga_val))
                elif Decimal(str(chg_val)) < 0:
                    rtn_price = Decimal(str(rtn_price)) - Decimal(str(hoga_val))
                else:
                    break

                if Decimal(str(chg_val)) > 0:
                    if Decimal(str(rtn_price)) >= Decimal(str(st_price)) * (
                            Decimal(str(1)) + (Decimal(str(chg_val))) / Decimal(str(100))):
                        break
                elif Decimal(str(chg_val)) < 0:
                    if Decimal(str(rtn_price)) <= Decimal(str(st_price)) * (
                            Decimal(str(1)) + (Decimal(str(chg_val))) / Decimal(str(100))):
                        break

        return rtn_price

    # ----------------------------------------
    # Exception Raise
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : get_hoga
# - Desc : 호가 금액 계산
# - Input
#   1) cur_price : 현재가격
# - Output
#   1) hoga_val : 호가단위
# -----------------------------------------------------------------------------
def get_hoga(cur_price):
    try:

        # 호가 단위
        if Decimal(str(cur_price)) < 10:
            hoga_val = 0.01
        elif Decimal(str(cur_price)) < 100:
            hoga_val = 0.1
        elif Decimal(str(cur_price)) < 1000:
            hoga_val = 1
        elif Decimal(str(cur_price)) < 10000:
            hoga_val = 5
        elif Decimal(str(cur_price)) < 100000:
            hoga_val = 10
        elif Decimal(str(cur_price)) < 500000:
            hoga_val = 50
        elif Decimal(str(cur_price)) < 1000000:
            hoga_val = 100
        elif Decimal(str(cur_price)) < 2000000:
            hoga_val = 500
        else:
            hoga_val = 1000

        return hoga_val

    # ----------------------------------------
    # Exception Raise
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : get_krwbal
# - Desc : KRW 잔고 조회
# - Input
# - Output
#   1) KRW 잔고 Dictionary
#     1. krw_balance : KRW 잔고
#     2. fee : 수수료
#     3. available_krw : 매수가능 KRW잔고(수수료를 고려한 금액)
# -----------------------------------------------------------------------------
def get_krwbal():
    try:

        # 잔고 리턴용
        rtn_balance = {}

        # 수수료 0.05%(업비트 기준)
        fee_rate = 0.05

        payload = {
            'access_key': access_key,
            'nonce': str(uuid.uuid4()),
        }

        jwt_token = jwt.encode(payload, secret_key)
        authorize_token = 'Bearer {}'.format(jwt_token)
        headers = {"Authorization": authorize_token}

        res = send_request("GET", server_url + "/v1/accounts", "", headers)

        data = res.json()

        logging.debug(data)

        for dataFor in data:
            if (dataFor['currency']) == "KRW":
                krw_balance = math.floor(Decimal(str(dataFor['balance'])))

        # 잔고가 있는 경우만
        if Decimal(str(krw_balance)) > Decimal(str(0)):
            # 수수료
            fee = math.ceil(Decimal(str(krw_balance)) * (Decimal(str(fee_rate)) / Decimal(str(100))))

            # 매수가능금액
            available_krw = math.floor(Decimal(str(krw_balance)) - Decimal(str(fee)))

        else:
            # 수수료
            fee = 0

            # 매수가능금액
            available_krw = 0

        # 결과 조립
        rtn_balance['krw_balance'] = krw_balance
        rtn_balance['fee'] = fee
        rtn_balance['available_krw'] = available_krw

        return rtn_balance

    # ----------------------------------------
    # Exception Raise
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : get_accounts
# - Desc : 잔고정보 조회
# - Input
#   1) except_yn : KRW 및 소액 제외
#   2) market_code : 마켓코드 추가(매도시 필요)
# - Output
#   1) 잔고 정보
# -----------------------------------------------------------------------------
# 계좌 조회
def get_accounts(except_yn, market_code):
    try:

        rtn_data = []

        # 해당 마켓에 존재하는 종목 리스트만 추출
        market_item_list = get_items(market_code, '')

        # 소액 제외 기준
        min_price = 5000

        payload = {
            'access_key': access_key,
            'nonce': str(uuid.uuid4()),
        }

        jwt_token = jwt.encode(payload, secret_key)
        authorize_token = 'Bearer {}'.format(jwt_token)
        headers = {"Authorization": authorize_token}

        res = send_request("GET", server_url + "/v1/accounts", "", headers)
        account_data = res.json()

        for account_data_for in account_data:
            for market_item_list_for in market_item_list:

                # 해당 마켓에 있는 종목만 조합
                if market_code + '-' + account_data_for['currency'] == market_item_list_for['market']:

                    # KRW 및 소액 제외
                    if except_yn == "Y" or except_yn == "y":
                        if account_data_for['currency'] != "KRW" and Decimal(str(account_data_for['avg_buy_price'])) * (
                                Decimal(str(account_data_for['balance'])) + Decimal(
                            str(account_data_for['locked']))) >= Decimal(str(min_price)):
                            rtn_data.append(
                                {'market': market_code + '-' + account_data_for['currency'],
                                 'balance': account_data_for['balance'],
                                 'locked': account_data_for['locked'],
                                 'avg_buy_price': account_data_for['avg_buy_price'],
                                 'avg_buy_price_modified': account_data_for['avg_buy_price_modified']})
                    else:
                        if account_data_for['currency'] != "KRW":
                            rtn_data.append(
                                {'market': market_code + '-' + account_data_for['currency'],
                                 'balance': account_data_for['balance'],
                                 'locked': account_data_for['locked'],
                                 'avg_buy_price': account_data_for['avg_buy_price'],
                                 'avg_buy_price_modified': account_data_for['avg_buy_price_modified']})

        return rtn_data

    # ----------------------------------------
    # Exception Raise
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : chg_account_to_comma
# - Desc : 잔고 종목 리스트를 콤마리스트로 변경
# - Input
#   1) account_data : 잔고 데이터
# - Output
#   1) 종목 리스트(콤마 구분자)
# -----------------------------------------------------------------------------
def chg_account_to_comma(account_data):
    try:

        rtn_data = ""

        for account_data_for in account_data:

            if rtn_data == '':
                rtn_data = rtn_data + account_data_for['market']
            else:
                rtn_data = rtn_data + ',' + account_data_for['market']

        return rtn_data

    # ----------------------------------------
    # Exception Raise
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : get_ticker
# - Desc : 현재가 조회
# - Input
#   1) target_itemlist : 대상 종목(콤마 구분자)
# - Output
#   2) 현재가 데이터
# -----------------------------------------------------------------------------
def get_ticker(target_itemlist):
    try:

        url = "https://api.upbit.com/v1/ticker"

        querystring = {"markets": target_itemlist}
        response = send_request("GET", url, querystring, "")

        logging.info(response)
        rtn_data = response.json()

        return rtn_data

    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : cancel_order
# - Desc : 미체결 주문 취소
# - Input
#   1) target_item : 대상종목
#   2) side : 매수/매도 구분(BUY/bid:매수, SELL/ask:매도, ALL:전체)
# - Output
# -----------------------------------------------------------------------------
def cancel_order(target_item, side):
    try:

        # 미체결 주문 조회
        order_data = get_order(target_item)

        # 매수/매도 구분
        for order_data_for in order_data:

            if side == "BUY" or side == "buy":
                if order_data_for['side'] == "ask":
                    order_data.remove(order_data_for)
            elif side == "SELL" or side == "sell":
                if order_data_for['side'] == "bid":
                    order_data.remove(order_data_for)

        # 미체결 주문이 있으면
        if len(order_data) > 0:

            # 미체결 주문내역 전체 취소
            for order_data_for in order_data:
                cancel_order_uuid(order_data_for['uuid'])

    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : cancel_order_uuid
# - Desc : 미체결 주문 취소 by UUID
# - Input
#   1) order_uuid : 주문 키
# - Output
#   1) 주문 내역 취소
# -----------------------------------------------------------------------------
def cancel_order_uuid(order_uuid):
    try:

        query = {
            'uuid': order_uuid,
        }

        query_string = urlencode(query).encode()

        m = hashlib.sha512()
        m.update(query_string)
        query_hash = m.hexdigest()

        payload = {
            'access_key': access_key,
            'nonce': str(uuid.uuid4()),
            'query_hash': query_hash,
            'query_hash_alg': 'SHA512',
        }

        jwt_token = jwt.encode(payload, secret_key)
        authorize_token = 'Bearer {}'.format(jwt_token)
        headers = {"Authorization": authorize_token}

        res = send_request("DELETE", server_url + "/v1/order", query, headers)
        rtn_data = res.json()

        return rtn_data

    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : get_order
# - Desc : 미체결 주문 조회
# - Input
#   1) target_item : 대상종목
# - Output
#   1) 미체결 주문 내역
# -----------------------------------------------------------------------------
def get_order(target_item):
    try:
        query = {
            'market': target_item,
            'state': 'wait',
        }

        query_string = urlencode(query).encode()

        m = hashlib.sha512()
        m.update(query_string)
        query_hash = m.hexdigest()

        payload = {
            'access_key': access_key,
            'nonce': str(uuid.uuid4()),
            'query_hash': query_hash,
            'query_hash_alg': 'SHA512',
        }

        jwt_token = jwt.encode(payload, secret_key)
        authorize_token = 'Bearer {}'.format(jwt_token)
        headers = {"Authorization": authorize_token}

        res = send_request("GET", server_url + "/v1/orders", query, headers)
        rtn_data = res.json()

        return rtn_data

    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : get_order
# - Desc : 미체결 주문 조회
# - Input
#   1) side : 주문상태
# - Output
#   1) 주문 내역 리스트
# -----------------------------------------------------------------------------
def get_order_list(side):
    try:
        query = {
            'state': side,
        }

        query_string = urlencode(query).encode()

        m = hashlib.sha512()
        m.update(query_string)
        query_hash = m.hexdigest()

        payload = {
            'access_key': access_key,
            'nonce': str(uuid.uuid4()),
            'query_hash': query_hash,
            'query_hash_alg': 'SHA512',
        }

        jwt_token = jwt.encode(payload, secret_key)
        authorize_token = 'Bearer {}'.format(jwt_token)
        headers = {"Authorization": authorize_token}

        res = send_request("GET", server_url + "/v1/orders", query, headers)
        rtn_data = res.json()

        return rtn_data

    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception:
        raise




# -----------------------------------------------------------------------------
# - Name : get_order_status
# - Desc : 주문 조회(상태별)
# - Input
#   1) target_item : 대상종목
#   2) status : 주문상태(wait : 체결 대기, watch : 예약주문 대기, done : 전체 체결 완료, cancel : 주문 취소)
# - Output
#   1) 주문 내역
# -----------------------------------------------------------------------------
def get_order_status(target_item, status):
    try:

        query = {
            'market': target_item,
            'state': status,
        }

        query_string = urlencode(query).encode()

        m = hashlib.sha512()
        m.update(query_string)
        query_hash = m.hexdigest()

        payload = {
            'access_key': access_key,
            'nonce': str(uuid.uuid4()),
            'query_hash': query_hash,
            'query_hash_alg': 'SHA512',
        }

        jwt_token = jwt.encode(payload, secret_key)
        authorize_token = 'Bearer {}'.format(jwt_token)
        headers = {"Authorization": authorize_token}

        res = send_request("GET", server_url + "/v1/orders", query, headers)
        rtn_data = res.json()

        return rtn_data

    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : orderby_dict
# - Desc : 딕셔너리 정렬
# - Input
#   1) target_dict : 정렬 대상 딕셔너리
#   2) target_column : 정렬 대상 딕셔너리
#   3) order_by : 정렬방식(False:오름차순, True,내림차순)
# - Output
#   1) 정렬된 딕서너리
# -----------------------------------------------------------------------------
def orderby_dict(target_dict, target_column, order_by):
    try:

        rtn_dict = sorted(target_dict, key=(lambda x: x[target_column]), reverse=order_by)

        return rtn_dict

    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : filter_dict
# - Desc : 딕셔너리 필터링
# - Input
#   1) target_dict : 정렬 대상 딕셔너리
#   2) target_column : 정렬 대상 컬럼
#   3) filter : 필터
# - Output
#   1) 필터링된 딕서너리
# -----------------------------------------------------------------------------
def filter_dict(target_dict, target_column, filter):
    try:

        for target_dict_for in target_dict[:]:
            if target_dict_for[target_column] != filter:
                target_dict.remove(target_dict_for)

        return target_dict

    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : get_order_chance
# - Desc : 주문 가능정보 조회
# - Input
#   1) target_item : 대상종목
# - Output
#   1) 주문 가능 정보
# -----------------------------------------------------------------------------
def get_order_chance(target_item):
    try:
        query = {
            'market': target_item,
        }

        query_string = urlencode(query).encode()

        m = hashlib.sha512()
        m.update(query_string)
        query_hash = m.hexdigest()

        payload = {
            'access_key': access_key,
            'nonce': str(uuid.uuid4()),
            'query_hash': query_hash,
            'query_hash_alg': 'SHA512',
        }

        jwt_token = jwt.encode(payload, secret_key)
        authorize_token = 'Bearer {}'.format(jwt_token)
        headers = {"Authorization": authorize_token}

        res = send_request("GET", server_url + "/v1/orders/chance", query, headers)
        rtn_data = res.json()

        return rtn_data

    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : get_max_min
# - Desc : MAX/MIN 값 조회
# - Input
#   1) candle_datas : 캔들 정보
#   2) col_name : 대상 컬럼
# - Output
#   1) MAX 값
#   2) MIN 값
# -----------------------------------------------------------------------------
def get_max(candle_data, col_name_high, col_name_low):
    try:
        # MA 데이터 리턴용
        max_min_list = []

        df = pd.DataFrame(candle_data)
        df = df.iloc[::-1]

        # MAX 계산

        max = np.max(df[col_name_high])
        min = np.min(df[col_name_low])

        max_min_list.append(
            {"MAX": max, "MIN": min})

        return max_min_list

    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : send_line_msg
# - Desc : 라인 메세지 전송
# - Input
#   1) message : 메세지
# - Output
#   1) response : 발송결과(200:정상)
# -----------------------------------------------------------------------------
def send_line_message(message):
    try:
        headers = {'Authorization': 'Bearer ' + line_token}
        data = {'message': message}

        response = requests.post(line_target_url, headers=headers, data=data)

        logging.debug(response)

        return response

    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception:
        raise




# -----------------------------------------------------------------------------
# - Name : send_msg
# - Desc : 메세지 전송
# - Input
#   1) sent_list : 메세지 발송 내역
#   2) key : 메세지 키
#   3) contents : 메세지 내용
#   4) msg_intval : 메세지 발송주기
# - Output
#   1) sent_list : 메세지 발송 내역
# -----------------------------------------------------------------------------
def send_msg(sent_list, key, contents, msg_intval):
    try:

        # msg_intval = 'N' 이면 메세지 발송하지 않음
        if msg_intval.upper() != 'N':

            # 발송여부 체크
            sent_yn = False

            # 발송이력
            sent_dt = ''

            # 발송내역에 해당 키 존재 시 발송 이력 추출
            for sent_list_for in sent_list:
                if key in sent_list_for.values():
                    sent_yn = True
                    sent_dt = datetime.strptime(sent_list_for['SENT_DT'], '%Y-%m-%d %H:%M:%S')

            # 기 발송 건
            if sent_yn:

                logging.info('기존 발송 건')

                # 현재 시간 추출
                current_dt = datetime.strptime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')

                # 시간 차이 추출
                diff = current_dt - sent_dt

                # 발송 시간이 지난 경우에는 메세지 발송
                if diff.seconds >= int(msg_intval):

                    logging.info('발송 주기 도래 건으로 메시지 발송 처리!')

                    # 메세지 발송
                    send_line_message(contents)

                    # 기존 메시지 발송이력 삭제
                    for sent_list_for in sent_list[:]:
                        if key in sent_list_for.values():
                            sent_list.remove(sent_list_for)

                    # 새로운 발송이력 추가
                    sent_list.append({'KEY': key, 'SENT_DT': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})

                else:
                    logging.info('발송 주기 미 도래 건!')

            # 최초 발송 건
            else:
                logging.info('최초 발송 건')

                # 메세지 발송
                send_line_message(contents)

                # 새로운 발송이력 추가
                sent_list.append({'KEY': key, 'SENT_DT': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})

        return sent_list

    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception:
        raise


# -----------------------------------------------------------------------------
# - Name : read_file
# - Desc : 파일 읽기
# - Input
# 1. name : 파일 명
# - Output
# 1. 파일 내용
# -----------------------------------------------------------------------------
def read_file(name):
    try:

        path = './conf/' + str(name) + '.txt'

        f = open(path, 'r')
        line = f.readline()
        f.close()

        logging.debug(line)

        contents = line

        return contents

    # ----------------------------------------
    # 모든 함수의 공통 부분(Exception 처리)
    # ----------------------------------------
    except Exception:
        raise