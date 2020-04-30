__author__ = "Wenjie Chen"
__email__ = "wc2685@columbia.edu"

import os
from multiprocessing import Process

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

STORE_DIR = os.path.join(os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "res")


class template_0:
    """
    The first template to analyze CRD 
    """

    def __init__(self, IP="localhost", interval=10, port=9000):
        # create spark context
        self.spark = SparkSession.builder.appName('template0').getOrCreate()
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
        """
        This function is to read n lines data from redis, then count the call time duration sum of every hour.
        """

        def updateFunc(new_values, last_sum):
            return sum(new_values) + (last_sum or 0)
        self.lines = self.lines.filter(lambda x: x)
        id_time_duration = self.lines.map(
            (lambda x: (x.split("|")[2], x.split("|")[4], x.split("|")[5])))
        temp_id_duration = id_time_duration.map(
            lambda x: (x[1].split(" ")[3].split(":")[0], int(x[2])))
        temp_id_duration_total = temp_id_duration.reduceByKey(
            lambda x, y: x + y).updateStateByKey(updateFunc)

        temp_id_duration_total.pprint()
        temp_id_duration_total.foreachRDD(
            lambda rdd: rdd.sortBy(lambda x: x[0]).toDF().toPandas().to_json(
                os.path.join(STORE_DIR, "tmp0",
                             "tmp0.json")) if not rdd.isEmpty() else None)


def template_0_main():
    test_temp_0 = template_0(IP="localhost", port=9000)
    test_temp_0.count_duration(None)
    test_temp_0.ssc.checkpoint(
        os.path.join(os.path.dirname(STORE_DIR), "checkpoints"))
    test_temp_0.ssc.start()
    print("Start process 0 for template 0")
    # test_temp_0.ssc.stop(stopSparkContext=False, stopGraceFully=True)
    test_temp_0.ssc.awaitTermination()  # used for real time


if __name__ == '__main__':
    p0 = Process(target=template_0_main)
    p0.start()
    print("Wait for terminated")
    p0.join()
