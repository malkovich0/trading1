import multiprocessing as mp
import time

def mp1(list_test):
    while True:

        print('mp', list_test)
        list_test[:] = list_test[1:]
        # for i in list_test:
        #     print(i)
        time.sleep(1)

def mp2(list_test):
    while True:
        # list_test.append(len(list_test)+1)
        time.sleep(3)

def p3(list_test):
    while True:
        list_test.append('a')
        time.sleep(0.7)
        list_test.append('b')
        time.sleep(0.7)
        print('p',list_test)

if __name__ == '__main__':
    list_test = mp.Manager().list([1,2,3,4,5])
    print(list_test)
    # def test(list_test):
    p1 = mp.Process(name='p1', target=mp1, args=(list_test,),daemon=True)
    p2 = mp.Process(name='p2', target=mp2, args=(list_test,),daemon=True)
    p1.start()
    p2.start()
    p3(list_test)
    p1.join()
    p2.join()
    # print(list_test)