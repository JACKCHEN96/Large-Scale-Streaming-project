from pyspark import SparkContext
import time
import matplotlib.pyplot as plt
import numpy as np

def mapFun(x):
    return x


sc = SparkContext()
data = sc.textFile('test.txt')
data = data.persist()
data = data.map(lambda x: int(x))

X = []
Y1 = []
Y2 = []

B_start = time.time()
dataB = data.filter(lambda x: x % 10 ==1)
numB = dataB.count()
time.sleep(numB/100000000)
B_end = time.time()
time_B = (B_end-B_start)*10

C_start = time.time()
dataC = data.filter(lambda x: x % 10 ==5)
numC = dataC.count()
time.sleep(numC/100000000)
C_end = time.time()
time_C = (C_end-C_start)*10

for i in range(100):
    if i == 0:
        X.append(0)
        Y1.append(1)
        Y2.append(1)
    else:
        dataA = data.sample(False, i/100)
        #A_start = time.time()
        numA = dataA.count()
        print(numA)
        # time.sleep(numA/10000000)
        #A_end = time.time()
        time_A = numA/10000000*10000
        
        Y1.append(1)
        Y2.append((time_A+time_B+time_C)/(2*time_A+time_B+time_C))
        X.append(time_A/(time_A+time_B+time_C))
        Y1.append(1)
        X.append(i/100)
        Y2.append(1/(1+i/100))

plt.figure()
plt.plot(X, Y1, 'b', X, Y2, 'r')
plt.ylabel('throughput')
plt.xlabel('fraction of cost in shared subgraph')
plt.show()