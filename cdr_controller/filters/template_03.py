__author__ = "Jiajing Sun"
__email__ = "js5504@columbia.edu"

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

class template_03:
    """
    The third template to analyze the country of callednumber
    """

    def __init__(self, IP="localhost", interval=10, port=9003):
        self.spark = SparkSession.builder.appName('template0').getOrCreate()
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

    def count_duration(self, n):
        """
        This function is to read n lines data from redis, then count the call time duration sum of every hour.
        """

        # Drop all invalid data. TODO

        callednumber = self.lines.map(lambda x: (x.split("|")[3]))
        place = callednumber.map(lambda x: region_code_for_country_code(int(x.split("-")[0].split("+")[1])))
        place_count = place.map(lambda place: (place, 1)).reduceByKey(lambda x, y: x + y)

        place_count.pprint()

        place_count.foreachRDD(lambda rdd: rdd.sortBy(lambda x: x[0]).toDF().toPandas().to_json(os.path.join(STORE_DIR, "tmp3", "region.json")) if not rdd.isEmpty() else None)


def template_3_main():
    test_temp_3 = template_03(IP="localhost", port=9003)
    test_temp_3.count_duration(None)

    test_temp_3.ssc.start()
    print("Start process 0 for template 3")
    time.sleep(60)
    # test_temp_0.ssc.stop(stopSparkContext=False, stopGraceFully=True)
    test_temp_3.ssc.awaitTermination() # used for real time


if __name__ == '__main__':
    p3 = Process(target=template_3_main)
    p3.start()
    print("Wait for terminated")
    p3.join()