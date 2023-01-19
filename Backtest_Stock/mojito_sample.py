import mojito

key = 'PS8ZQRZa3hImKZ7YNvdhK08I55UpJKSwQ3sI'
secret = 'dUiGmIpCcE1qkxwE220r5VM/na7vOuNMxukSkO8UmLyH+vXTDvsElR0TqjUMSFFq2u/5ot/c4S9Lw9jlrFad8WkIZ8KUockGJ6iN3cFHGXMf5X3EBYazXNEO6s7mf7wRzLP4VQSPzlz82WgcRSWVdTc5exFmJtWVn9f9ZwXrkOIbM+IV1pU='
acc_no = "46506498-01"

if __name__ == "__main__":
    broker = mojito.KoreaInvestment(api_key=key, api_secret=secret, acc_no=acc_no)
    resp = broker._fetch_today_1m_ohlcv("005930",'122500')
    rtn = resp['output2']
    rtn = rtn[::-1]
    print(rtn)
    a = np.array([1, 2, 3, 4])
    np.savetxt('test1.txt', a, fmt='%d')
    b = np.loadtxt('test1.txt', dtype=int)


    def json_to_array(resp):
        return np.array([list(i.values()) for i in resp])


    # 전일자료 loading,

    if __name__ == "__main__":
        broker = mojito.KoreaInvestment(api_key=key, api_secret=secret, acc_no=acc_no)

        code_stock = '005930'
        if 파일 없으면,
        rtn = broker.fetch_today_1m_ohlcv(code_stock)
        ar_temp = json_to_array(rtn)
        np.savetxt(file_name, ar_temp[:-1], fmt='%d')  # 마지막 값은 아직 바뀌는 중이기때문에
    else:
        ar_ex = np.loadtxt(file_name, dtype=int)
        date_final = ar_ex[-1][0]
        time_final = ar_ex[-1][1]
        if date_final이 오늘보다 과거라면,
        새로저장
    else:
    time_final
    이후
    자료
    중
    최신하나빼고
    저장.
resp = broker._fetch_today_1m_ohlcv(code_stock, '122500')
rtn = resp['output2'][::-1]
rtn = rtn
print(rtn)

시간은
끝나는
시간
ex.
0
93000
이면
0
92900 - 0
93000
의
가격
    # print(resp)
    # broker_ws = mojito.KoreaInvestmentWS(key, secret, ["H0STCNT0", "H0STASP0"], ["005930", "000660"])
    # broker_ws.start()
    # while True:
    #     data_ = broker_ws.get()
    #     if data_[0] == '체결':
    #         print(data_[1])
        # elif data_[0] == '호가':
        #     print(data_[1])
        # elif data_[0] == '체잔':
        #     print(data_[1])
