import socket

s0 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s0.connect(('localhost',9000))
while True:
    try:
        r = s0.recv(1024)
    except OSError:
        print("Nothing")
    else:
        if r != b'':
            print(r)
        else:
            print('wait')
        s0.close()
        s0 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s0.connect(('localhost',9000))