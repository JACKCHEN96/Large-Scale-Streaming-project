__author__ = "Wenjie Chen"
__email__ = "wc2685@columbia.edu"

import redis
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import time

# rds_temp = redis.Redis(host='localhost', port=6379, decode_responses=True,
#                   db=6)  # host是redis主机，需要redis服务端和客户端都启动 redis默认端口是6379

# create spark context
spark = SparkSession.builder.appName('myApp').getOrCreate()
sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

# create sql context, used for saving rdd
sql_context = SparkSession(sc)

# create the Streaming Context from the above spark context with batch interval size (seconds)
ssc = StreamingContext(sc, 10)


class template_0:
    """
    The first template to analyze CRD 
    """

    def __init__(self,IP="localhost",interval=10,port=9000):
        self.IP=IP
        self.interval=interval
        self.port=port


        # read data from port

        self.lines = ssc.socketTextStream(self.IP, self.port)

        # 11111111
        # self.lines.foreachRDD(lambda rdd: print(rdd.take(20)))

        # 22222222
        # self.lines.pprint()

    def __str__(self):
        pass

    def count_duration(self,n):
        """
        This function is to read n lines data from redis, then count the call time duration sum of every hour.
        """
        
        id_time_duration = self.lines.map((lambda x: (x.split("|")[2], x.split("|")[4], x.split("|")[5])))

        # for i in range(24):
        #     # print("%d hour" % i)
        #     # temp_id_time_duration = id_time_duration.filter(lambda x: x[1].split(" ")[3].split(":")[0] == "%d" % i)
        temp_id_time_duration = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 0)
        temp_id_duration = temp_id_time_duration.map(lambda x: ("%d" % 0, int(x[2])))
        temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
        temp_id_duration_total.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 1)
        temp_id_duration = temp_id_time_duration.map(lambda x: ("%d" % 1, int(x[2])))
        temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
        temp_id_duration_total.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 2)
        temp_id_duration = temp_id_time_duration.map(lambda x: ("%d" % 2, int(x[2])))
        temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
        temp_id_duration_total.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 3)
        temp_id_duration = temp_id_time_duration.map(lambda x: ("%d" % 3, int(x[2])))
        temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
        temp_id_duration_total.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 4)
        temp_id_duration = temp_id_time_duration.map(lambda x: ("%d" % 4, int(x[2])))
        temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
        temp_id_duration_total.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 5)
        temp_id_duration = temp_id_time_duration.map(lambda x: ("%d" % 5, int(x[2])))
        temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
        temp_id_duration_total.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 6)
        temp_id_duration = temp_id_time_duration.map(lambda x: ("%d" % 6, int(x[2])))
        temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
        temp_id_duration_total.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 7)
        temp_id_duration = temp_id_time_duration.map(lambda x: ("%d" % 7, int(x[2])))
        temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
        temp_id_duration_total.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 8)
        temp_id_duration = temp_id_time_duration.map(lambda x: ("%d" % 8, int(x[2])))
        temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
        temp_id_duration_total.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 9)
        temp_id_duration = temp_id_time_duration.map(lambda x: ("%d" % 9, int(x[2])))
        temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
        temp_id_duration_total.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 10)
        temp_id_duration = temp_id_time_duration.map(lambda x: ("%d" % 10, int(x[2])))
        temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
        temp_id_duration_total.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 11)
        temp_id_duration = temp_id_time_duration.map(lambda x: ("%d" % 11, int(x[2])))
        temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
        temp_id_duration_total.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 12)
        temp_id_duration = temp_id_time_duration.map(lambda x: ("%d" % 12, int(x[2])))
        temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
        temp_id_duration_total.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 13)
        temp_id_duration = temp_id_time_duration.map(lambda x: ("%d" % 13, int(x[2])))
        temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
        temp_id_duration_total.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 14)
        temp_id_duration = temp_id_time_duration.map(lambda x: ("%d" % 14, int(x[2])))
        temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
        temp_id_duration_total.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 15)
        temp_id_duration = temp_id_time_duration.map(lambda x: ("%d" % 15, int(x[2])))
        temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
        temp_id_duration_total.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 16)
        temp_id_duration = temp_id_time_duration.map(lambda x: ("%d" % 16, int(x[2])))
        temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
        temp_id_duration_total.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 17)
        temp_id_duration = temp_id_time_duration.map(lambda x: ("%d" % 17, int(x[2])))
        temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
        temp_id_duration_total.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 18)
        temp_id_duration = temp_id_time_duration.map(lambda x: ("%d" % 18, int(x[2])))
        temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
        temp_id_duration_total.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 19)
        temp_id_duration = temp_id_time_duration.map(lambda x: ("%d" % 19, int(x[2])))
        temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
        temp_id_duration_total.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 20)
        temp_id_duration = temp_id_time_duration.map(lambda x: ("%d" % 20, int(x[2])))
        temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
        temp_id_duration_total.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 21)
        temp_id_duration = temp_id_time_duration.map(lambda x: ("%d" % 21, int(x[2])))
        temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
        temp_id_duration_total.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 22)
        temp_id_duration = temp_id_time_duration.map(lambda x: ("%d" % 22, int(x[2])))
        temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
        temp_id_duration_total.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 23)
        temp_id_duration = temp_id_time_duration.map(lambda x: ("%d" % 23, int(x[2])))
        temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
        temp_id_duration_total.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 24)
        temp_id_duration = temp_id_time_duration.map(lambda x: ("%d" % 24, int(x[2])))
        temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
        temp_id_duration_total.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))



test_temp_0=template_0(IP="localhost",port=9000)
test_temp_0.count_duration(None)

ssc.start()
time.sleep(60)
ssc.stop(stopSparkContext=False, stopGraceFully=True)
