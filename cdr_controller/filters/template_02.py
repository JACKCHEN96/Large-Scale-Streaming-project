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
from phonenumbers.phonenumberutil import region_code_for_country_code

STORE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "res")

class template_02:
    """
    The third template to analyze the country of callednumber
    """

    def __init__(self, IP="localhost", interval=10, port=9003):
        self.spark = SparkSession.builder.appName('template2').getOrCreate()
        self.sc = SparkContext.getOrCreate(SparkConf().setMaster("local"))

        # create sql context, used for saving rdd
        self.sql_context = SparkSession(self.sc)

        # create the Streaming Context from the above spark context with batch interval size (seconds)
        self.ssc = StreamingContext(self.sc, 10)
        self.IP=IP
        self.interval=interval
        self.port=port

        # read data from port

        self.lines = self.ssc.socketTextStream(self.IP, self.port)

    def __str__(self):
        pass

    def count_tag(self, n):
        """
        This function is to read n lines data from redis, then count the call time duration sum of every hour.
        """

        # Drop all invalid data. TODO

        def helper(x):
            rds_type = redis.Redis(host="localhost", port=6379, decode_responses=True,
                                   db=1)  # host是redis主机，需要redis服务端和客户端都启动 redis默认端口是6379

            return (x.split("|")[0],"private") if rds_type.get(x.split("|")[3])==None else (x.split("|")[0],rds_type.get(x.split("|")[3]))

        process_lines=self.lines.map(helper)
        process_lines.pprint()
        process_lines.countByValue().pprint()
        process_lines.countByValue().reduceByKey(lambda x,y: x+y).pprint()

        # resultstream = (process_lines
        #                 .map(lambda word: (word, 1))  # 将word映射成(word,1)
        #                 .reduceByKey(lambda x, y: x + y))  # reduceByKey对所有有着相同key的items执行reduce操作
        #
        # resultstream.pprint()
        #
        # resultstream.foreachRDD(lambda rdd: rdd.sortBy(lambda x: x[0]).toDF().toPandas().to_json(os.path.join(STORE_DIR, "tmp2", "tag.json")) if not rdd.isEmpty() else None)


def template_2_main():
    test_temp_2 = template_02(IP="localhost", port=9002)
    test_temp_2.count_tag(None)

    test_temp_2.ssc.start()
    print("Start process 0 for template 2")
    time.sleep(60)
    # test_temp_0.ssc.stop(stopSparkContext=False, stopGraceFully=True)
    test_temp_2.ssc.awaitTermination() # used for real time


if __name__ == '__main__':
    p2 = Process(target=template_2_main)
    p2.start()
    print("Wait for terminated")
    p2.join()