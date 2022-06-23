from module import upbit

upbit.send_line_message('TEST')

# import pickle
# with open('./info/login_info.pickle','rb') as handle:
#     login_info = pickle.load(handle)
# access_key = login_info['access_key']
# secret_key = login_info['secret_key']
# server_url = login_info['server_url']
# line_target_url = login_info['line_target_url']
# line_token = login_info['line_token']
# print(line_target_url, line_token)