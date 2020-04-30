__author__ = "Wenjie Chen"
__email__ = "wc2685@columbia.edu"

import os
import time
from multiprocessing import Process

from phonenumbers.phonenumberutil import region_code_for_country_code
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

STORE_DIR = os.path.join(os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "res")


class template_03:
    """
    The third template to analyze the country of callednumber
    """

    def __init__(self, IP="localhost", interval=10, port=9003):
        self.sc = SparkContext.getOrCreate(SparkConf().setMaster("local[2]"))

        # create sql context, used for saving rdd
        self.sql_context = SparkSession(self.sc)

        # create the Streaming Context from the above spark context with batch interval size (seconds)
        self.ssc = StreamingContext(self.sc, 10)
        self.IP = IP
        self.interval = interval
        self.port = port

        # read data from port

        self.lines = self.ssc.socketTextStream(self.IP, self.port)

    def __str__(self):
        pass

    def count_duration(self):

        def updateFunc(new_values, last_sum):
            return sum(new_values) + (last_sum or 0)

        # Drop all invalid data. TODO

        callednumber = self.lines.map(lambda x: (x.split("|")[3]))
        place = callednumber.map(lambda x: region_code_for_country_code(
            int(x.split("-")[0].split("+")[1])))
        place_count = place.map(lambda place: (place, 1)).reduceByKey(
            lambda x, y: x + y).updateStateByKey(updateFunc)

        place_count.pprint()

        place_count.foreachRDD(
            lambda rdd: rdd.sortBy(lambda x: x[0]).toDF().toPandas().to_json(
                os.path.join(STORE_DIR, "tmp3",
                             "region.json")) if not rdd.isEmpty() else None)


def template_3_main():
    test_temp_3 = template_03(IP="localhost", port=9003)
    test_temp_3.count_duration()
    test_temp_3.ssc.checkpoint(
        os.path.join(os.path.dirname(STORE_DIR), "checkpoints"))
    test_temp_3.ssc.start()
    print("Start process 3 for template 3")
    time.sleep(60)
    # test_temp_0.ssc.stop(stopSparkContext=False, stopGraceFully=True)
    test_temp_3.ssc.awaitTermination()  # used for real time


if __name__ == '__main__':
    p3 = Process(target=template_3_main)
    p3.start()
    print("Wait for terminated")
    p3.join()
