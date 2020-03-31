from pyspark import SparkContext
import time
import matplotlib.pyplot as plt

sc = SparkContext()
data = sc.textFile('test.txt')
data = data.persist()
data = data.map(lambda x: int(x))

X = [0]*100
Y1 = [1]*100
Y2 = [0]*100
num = data.count()
print(num,'num')

for i in range(100):

    selectivity = float(i)/100
    X[i] = selectivity

    # start A
    firstA_start = time.time()          # firstA_start
    
    dataA = data.filter(lambda x: x % 2 ==1) 
    for j1 in range(num):
        through_data_1 = dataA.map(lambda x: x)

    firstA_end = time.time()            # firstA_end
     
    # start B
    num_1 = dataA.count()               # correspond to 0.5 

    firstB_start = time.time()          # firstB_start

    dataB = dataA.filter(lambda x: x <= (4*i))
    for j2 in range(num_1):
        through_data_2 = dataB.map(lambda x: x)

    firstB_end = time.time()            # firstB_end

    time1 = (firstA_end - firstA_start) + (firstB_end - firstB_start)


    # reordering
    # start B
    secondB_start = time.time()         # secondB_start

    dataB2 = data.filter(lambda x: x <= (4*i))  
    for j3 in range(num):
        through_data_3 = dataB2.map(lambda x: x)

    secondB_end = time.time()           # secondB_end

    # start A
    num_2 = dataB2.count()              # correspond to selectivity 

    secondA_start = time.time()         # secondA_start

    dataA2 = dataB2.filter(lambda x: x % 2 ==1)
    for j4 in range(num_2):
        through_data_4 = dataA2.map(lambda x: x)

    secondA_end = time.time()           # secondA_end

    time2 = (secondB_end - secondB_start) + (secondA_end - secondA_start)


    print(i,"th round")

    Y2[i] = time1 / time2
    print(X[i],Y2[i])


plt.figure()
plt.plot(X, Y1, 'b', X, Y2, 'r')
plt.ylabel('throughput')
plt.xlabel('selectivity of B')
plt.show()

