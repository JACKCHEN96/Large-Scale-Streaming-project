import json
import socket
import redis
import time

rds = redis.Redis(host='localhost', port=6379, decode_responses=True,db=0)


class twitter_client:
    def __init__(self, TCP_IP, TCP_PORT):
        self.s = s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.bind((TCP_IP, TCP_PORT))

    def run_client(self, tags):
        try:
            self.s.listen(1)
            while True:
                print("Waiting for TCP connection...")
                conn, addr = self.s.accept()
                print("Connected... Starting getting tweets.")
                sendData(conn, tags)
                conn.close()
        except KeyboardInterrupt:
            exit

def test():
    if rds.llen('ID') != 0:
        l = rds.llen('ID')
        for i in range(l):
            t = rds.rpop('ID')
            b = rds.rpop('type')
            print(t)
        time.sleep(0.5)
    else:
        time.sleep(1)
        print('I love nine river')   

if __name__ == '__main__':
    # client = twitter_client("localhost", 9001)
    # client.run_client(tags)
    try:
        while True:
            test()
    except KeyboardInterrupt:
        exit
