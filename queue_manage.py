import json
import socket
import redis
import time
import threading

rds = redis.Redis(host='localhost', port=6379, decode_responses=True, db=0)
data = ""
flag = 0

def test():
    global flag, data
    if rds.llen('ID') != 0 and flag == 0:
        data = ""
        l = rds.llen('ID')
        for i in range(l):
            t = rds.rpop('ID')
            data += t
            data += '\n'
        flag = 1
    else:
        time.sleep(1)
        print('flag')

if __name__ == '__main__':
    s0 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s0.bind(('localhost', 9000))
    s1.bind(('localhost', 9001))
    s2.bind(('localhost', 9002))
    print('initial')
    try:
        print('initial 0')
        s0.listen(1)
        s1.listen(1)
        s2.listen(1)
        print('initial 1')
        while True:
            test()
            if flag != 0 and data != "":
                conn, addr = s0.accept()
                conn.sendall(data)
                conn.close()
                conn, addr = s1.accept()
                conn.sendall(data)
                conn.close()
                conn, addr = s2.accept()
                conn.sendall(data)
                conn.close()
                flag = 0
                print('send')
            else:
                print('wait')
    except KeyboardInterrupt:
        exit

