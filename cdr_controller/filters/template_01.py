__author__ = "Wenjie Chen"
__email__ = "wc2685@columbia.edu"

import os
from multiprocessing import Process

import redis
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

STORE_DIR = os.path.join(os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "res")


class template_01:
    """
    The second template to analyze CRD. It reads CRD from the first redis DB and then extract the call type from the second redis DB.
    """

    def __init__(self, IP="localhost", interval=10, port=9001):
        self.sc = SparkContext.getOrCreate(SparkConf().setMaster("local[2]"))


        # create sql context, used for saving rdd
        self.sql_context = SparkSession(self.sc)

        # create the Streaming Context from the above spark context with batch interval size (seconds)
        self.ssc = StreamingContext(self.sc, 1)
        self.IP = IP
        self.interval = interval
        self.port = port

        # read data from port

        self.lines = self.ssc.socketTextStream(self.IP, self.port)

    def __str__(self):
        pass

    def count_type(self):
        """
        This function is to read data and extract the call type.
        """

        # TODO. Drop all invalid data
        def helper(x):
            rds_type = redis.Redis(host="localhost", port=6379,
                                   decode_responses=True,
                                   db=1)  # host是redis主机，需要redis服务端和客户端都启动 redis默认端口是6379

            return "private" if rds_type.get(
                x.split("|")[3]) == None else rds_type.get(x.split("|")[3])

        process_lines = self.lines.map(helper)

        # self.lines.pprint()
        # process_lines.pprint()

        def updateFunc(new_values, last_sum):
            return sum(new_values) + (last_sum or 0)

        resultstream = process_lines.map(
            lambda word: (word.lower(), 1)).reduceByKey(
            lambda x, y: x + y).updateStateByKey(updateFunc)
        resultstream.pprint()

        resultstream.foreachRDD(
            lambda rdd: rdd.sortBy(lambda x: x[0]).toDF().toPandas().to_json(
                os.path.join(STORE_DIR, "tmp1",
                             "type.json")) if not rdd.isEmpty() else None)


def template_1_main():
    test_temp_1 = template_01(IP="localhost", port=9001)
    test_temp_1.count_type()
    test_temp_1.ssc.checkpoint(
        os.path.join(os.path.dirname(STORE_DIR), "checkpoints-1"))
    test_temp_1.ssc.start()
    print("Start process 1 for template 1")
    # test_temp_0.ssc.stop(stopSparkContext=False, stopGraceFully=True)
    test_temp_1.ssc.awaitTermination()  # used for real time


if __name__ == '__main__':
    p1 = Process(target=template_1_main)
    p1.start()
    print("Wait for terminated")
    p1.join()
