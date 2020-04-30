__author__ = "Wenjie Chen"
__email__ = "wc2685@columbia.edu"

import redis
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from multiprocessing import Process
import time
import os

STORE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "res")

class template_05:
    """
    The fifth template to count the call time duration sum of every hour of n lines
    """

    def __init__(self, IP="localhost", interval=30, port=9005):

        self.spark = SparkSession.builder.appName('template0').getOrCreate()
        self.sc = SparkContext.getOrCreate(SparkConf().setMaster("local"))

        # create sql context, used for saving rdd
        self.sql_context = SparkSession(self.sc)

        # create the Streaming Context from the above spark context with batch interval size (seconds)
        self.ssc = StreamingContext(self.sc, 30)
        self.IP=IP
        self.interval=interval
        self.port=port

        # read data from port

        self.lines = self.ssc.socketTextStream(self.IP, self.port)

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

        # people_calltime_w_count.pprint()
        # people_calltime_d_count.pprint()

        # people_calltime_count_total=people_calltime_w_count.union(people_calltime_d_count)
        # people_calltime_count_total.pprint()

        people_calltime_w_count.foreachRDD(lambda rdd: rdd.sortBy(lambda x: (x[0], -x[2], x[1])).map(lambda x: (x[0], x[1])).distinct().reduceByKey(lambda x, y: x)
                                           .sortBy(lambda x: x[0]).toDF().toPandas().to_json(os.path.join(STORE_DIR, "tmp5", "day.json")) if not rdd.isEmpty() else None)
        people_calltime_w_count.pprint()
        people_calltime_d_count.foreachRDD(lambda rdd: rdd.sortBy(lambda x: (x[0], -x[2], x[1])).map(lambda x: (x[0], x[1])).distinct().reduceByKey(lambda x, y: x)
                                           .sortBy(lambda x: x[0]).toDF().toPandas().to_json(os.path.join(STORE_DIR, "tmp5", "clock.json")) if not rdd.isEmpty() else None)
        people_calltime_d_count.pprint()


def template_05_main():
    test_temp_5 = template_05(IP="localhost", port=9005)
    test_temp_5.count_calltime(None)

    test_temp_5.ssc.start()
    print("Start process 0 for template 5")
    time.sleep(60)
    # test_temp_0.ssc.stop(stopSparkContext=False, stopGraceFully=True)
    test_temp_5.ssc.awaitTermination() # used for real time


if __name__ == '__main__':
    p5 = Process(target=template_05_main)
    p5.start()
    print("Wait for terminated")
    p5.join()