# step1.telegram 패키지의 Updater, CommandHandler 모듈 import
from telegram.ext import Updater
from telegram.ext import CommandHandler

import pyupbit
from module import upbit
upbit_api = pyupbit.Upbit(upbit.access_key_pc, upbit.secret_key_pc)
target = {'list_coins':['KRW-BTC','KRW-ETH']}
token_test = '5551503871:AAGpGczJlEXQeSUEMNUw5lXwhAKp0tUqPew'

# def producer3(upbit_api, target):

def main_telegram(upbit_api, target):
    # step2.Updater(유저의 입력을 계속 모니터링하는 역할), Dispatcher
    updater = Updater(token=token_test, use_context=True)
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

    # 전량매도
    # def call_sellall_order(update, context):

    # 미체결취소
    # def call_cancel_wait_order(update, context):

    # step4.위에서 정의한 함수를 실행할 CommandHandler 정의
    balance_handler = CommandHandler('balance', call_balance)
    wait_handler = CommandHandler('wait', call_wait_order)
    target_handler = CommandHandler('target', call_target)
    price_handler = CommandHandler('price', call_price)

    # step5.Dispatcher에 Handler를 추가
    dispatcher.add_handler(balance_handler)
    dispatcher.add_handler(wait_handler)
    dispatcher.add_handler(target_handler)
    dispatcher.add_handler(price_handler)

    # step6.Updater 실시간 입력 모니터링 시작(polling 개념)
    updater.start_polling()

if __name__ == '__main__':
    # upbit_api =

    main_telegram(upbit_api,target)