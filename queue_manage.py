import sys
import socket
import redis
import time

rds = redis.Redis(host='localhost', port=6379, decode_responses=True, db=0)
data = ""
flag = 0

def test(Name):
    global flag, data
    l = rds.llen(Name)
    if l > 50 and flag == 0:
        data = ""
        for i in range(50):
            t = rds.rpop(Name)
            data += t
            data += '\n'
        flag = 1
        print('test')
    else:
        time.sleep(1)

if __name__ == '__main__':
    args = sys.argv[1:]
    port = int(args[0])
    listName = args[1]
    s0 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s0.bind(('localhost', port))
    s0.listen(5)
    conn, addr = s0.accept()
    while True:
        test(Name = listName)
        print(flag)
        if r != b'':
            conn.close()
            conn, addr = s0.accept()
        else:
            if flag != 0 and data != "":
                conn.sendall(bytes(data, 'utf-8'))
                flag = 0
                print('send')
            else:
                print('wait')