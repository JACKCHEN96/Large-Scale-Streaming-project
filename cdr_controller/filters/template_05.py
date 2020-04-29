__author__ = "Wenjie Chen"
__email__ = "wc2685@columbia.edu"

import time
import redis
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

rds_temp = redis.Redis(host='localhost', port=6379, decode_responses=True,
                       db=6)  # host是redis主机，需要redis服务端和客户端都启动 redis默认端口是6379

# create spark context
spark = SparkSession.builder.appName('myApp').getOrCreate()
sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

# create sql context, used for saving rdd
sql_context = SparkSession(sc)

# create the Streaming Context from the above spark context with batch interval size (seconds)
ssc = StreamingContext(sc, 10)

class template_5:
    """
    The fifth template to count the call time duration sum of every hour of n lines
    """

    def __init__(self, IP="localhost", interval=10, port=9000):
        self.IP = IP
        self.interval = interval
        self.port = port

        # read data from port
        self.lines = ssc.socketTextStream(self.IP, self.port)

    def __str__(self):
        pass

    def count_calltime(self, n):
        """
        This function is to read n lines data from redis, then count the call time duration sum of every hour.
        """

        people_calltime = self.lines.map(
            lambda x: (x.split("|")[0], x.split("|")[4]))

        people_calltime_w = people_calltime.map(
            lambda x: (x[0] + ":" + x[1].split(" ")[0], 1))
        people_calltime_d = people_calltime.map(
            lambda x: (x[0] + ":" + x[1].split(" ")[3].split(":")[0], 1))

        people_calltime_w_count = people_calltime_w.reduceByKey(
            lambda x, y: x + y).map(
            lambda x: (x[0].split(":")[0], x[0].split(":")[1], x[1]))
        people_calltime_d_count = people_calltime_d.reduceByKey(
            lambda x, y: x + y).map(
            lambda x: (x[0].split(":")[0], x[0].split(":")[1], x[1]))

        # print(people_calltime.take(20))
        # print(people_calltime_w_count.take(20))
        # print(people_calltime_d_count.take(20))

        def save_rdd_1(rdd):
            if not rdd.isEmpty():
                df1 = rdd.sortBy(lambda x: (x[0], -x[2], x[1])) \
                    .map(lambda x: (x[0], x[1])).distinct().reduceByKey(lambda x, y: x) \
                    .toDF()
                # df.show()
                df1.write.format("org.apache.spark.sql.redis").option("table", "counts_5_1").option("key.column",
                                                                                                    "_1").save(
                    mode='overwrite')

        def save_rdd_2(rdd):
            if not rdd.isEmpty():
                df2 = rdd.sortBy(lambda x: (x[0], -x[2], x[1])) \
                    .map(lambda x: (x[0], x[1])).distinct().reduceByKey(lambda x, y: x) \
                    .toDF()
                # df.show()
                df2.write.format("org.apache.spark.sql.redis").option("table", "counts_5_2").option("key.column",
                                                                                                    "_1").save(
                    mode='overwrite')

        # people_calltime_w_count.foreachRDD(save_rdd_1)
        # people_calltime_d_count.foreachRDD(save_rdd_2)
        people_calltime_w_count.foreachRDD(lambda rdd: print(rdd.sortBy(lambda x: (x[0], -x[2], x[1])).map(lambda x: (x[0], x[1])).distinct().reduceByKey(lambda x, y: x).collect()))
        people_calltime_d_count.foreachRDD(lambda rdd: print(rdd.sortBy(lambda x: (x[0], -x[2], x[1])).map(lambda x: (x[0], x[1])).distinct().reduceByKey(lambda x, y: x).collect()))
        # people_calltime_w_count.pprint()
        # people_calltime_d_count.pprint()

test_temp_5 = template_5(IP="localhost",port=9000)
test_temp_5.count_calltime(None)

ssc.start()
time.sleep(60)
ssc.stop(stopSparkContext=False, stopGraceFully=True)