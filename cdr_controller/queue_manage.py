import socket
import time
from multiprocessing import Process

import redis

rds = redis.Redis(host='localhost', port=6379, decode_responses=True, db=0)


class QueueManager:

    def __init__(self, port, listName):
        self.port = port
        self.listName = listName
        self.data = ""
        self.flag = 0

    def test(self):
        l = rds.llen(self.listName)
        if l > 50 and self.flag == 0:
            self.data = ""
            for i in range(50):
                t = rds.rpop(self.listName)
                self.data += t
                self.data += '\n'
            self.flag = 1
            print('test')
        else:
            time.sleep(1)

    def run(self):
        s0 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s0.bind(('localhost', self.port))
        s0.listen(5)
        conn, addr = s0.accept()
        while True:
            self.test()
            print(self.flag)
            if self.flag != 0 and self.data != "":
                conn.sendall(bytes(self.data, 'utf-8'))
                self.flag = 0
                print('send')
            else:
                print('wait')


def queueStarter(port, listName):
    print("Start queue manager on %d for %s" % (port, listName))
    while 1:
        try:
            queue_manager = QueueManager(port, listName)
            queue_manager.run()
        except Exception as e:
            print(e)


if __name__ == '__main__':
    qm2 = Process(target=queueStarter, args=(9002, "ID_02"))
    qm2.start()
    qm2.join()
