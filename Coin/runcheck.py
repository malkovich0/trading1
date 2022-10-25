import os

process_read = os.popen("ps -ef | grep trade.py | grep -v 'grep'").readlines()
# ps -ef 명령어를 이용해서 현재 프로세스를 출력한 후, 그 중 run24h.py 문자열이 포함된 줄만 모은다.
# grep 명령어 자체도 프로세스에 나타나므로 grep -v를 이용해서 제외한다.
check_process = str(process_read)
text_location=check_process.find("trade.py")
# 만약 문자열이 없으면, 즉 프로세스가 존재하지 않을 경우에는 -1을 반환한다.
if ( text_location == -1 ):
    # upbit.send_telegram_message('trade.py simulation 미확인')
    os.system("python trade.py &")
    # 해당 프로그램을 다시 실행한다. 백그라운드에서 실행할 경우 &기호를 붙인다.
    # print("Program restarted!")
else:
    pass

process_read_ws = os.popen("ps -ef | grep save_tickdata.py | grep -v 'grep'").readlines()
check_process_ws = str(process_read_ws)
text_location_ws=check_process_ws.find("save_tickdata.py")

if ( text_location_ws == -1 ):
    # upbit.send_telegram_message('trade.py simulation 미확인')
    os.system("python save_tickdata.py &")
    # 해당 프로그램을 다시 실행한다. 백그라운드에서 실행할 경우 &기호를 붙인다.
    # print("Program restarted!")
else:
    pass