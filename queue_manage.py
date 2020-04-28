import json
import socket
import redis
import time
import threading

rds = redis.Redis(host='localhost', port=6379, decode_responses=True, db=0)
data = ""
flag = 0
addr_list = []

def test():
    global flag, data
    l = rds.llen('ID')
    if l > 50 and flag == 0:
        data = ""
        for i in range(50):
            t = rds.rpop('ID')
            data += t
            data += '\n'
        flag = 1
        print('test')
    else:
        time.sleep(1)

if __name__ == '__main__':
    s0 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s0.bind(('localhost', 9000))
    try:
        s0.listen(5)
        while True:
            test()
            print(flag)
            print(data)
            if flag != 0 and data != "":
                try:
                    conn, addr = s0.accept()
                except OSError:
                    print("Nothing")
                else:
                    print(addr)
                    conn.sendall(bytes(data, 'utf-8'))
                    flag = 0
                    print('send')
            else:
                print('wait')
    except KeyboardInterrupt:
        exit

