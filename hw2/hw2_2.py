from pyspark import SparkContext
import time
import matplotlib.pyplot as plt
import numpy as np

sc = SparkContext()
data = sc.textFile('test.txt')
data = data.persist()
data = data.map(lambda x: int(x))

X = [0]*100
Y1 = [1]*100
Y2 = [1]*100

num = data.count()
print(num,'num')
hist_standard=data.histogram(500)[1]

for i in range(100):
    selectivity = float(i)/100
    X[i] = selectivity

    if i != 0:
        data_new = data.sample(False, selectivity)
        hist_new = data_new.histogram(500)[1]
        hist_standard = np.array(hist_standard)/np.sum(hist_standard)
        hist_new = np.array(hist_new)/np.sum(hist_new)
        distance = np.sqrt(np.sum(np.square(hist_standard - hist_new)))
        k = np.sqrt(np.sum(np.square(hist_standard)))
        Y1[i] = 1-distance/k
        Y2[i] = 1/(selectivity*100)

plt.figure()
plt.semilogx(X, Y1, 'b', X, Y2, 'r')
plt.xlabel('selectivity')
plt.legend(['accuracy', 'throughput'], loc = 'upper right')
plt.show()