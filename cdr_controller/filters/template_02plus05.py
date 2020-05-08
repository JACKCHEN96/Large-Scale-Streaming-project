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
        self.IP = IP
        self.interval = interval
        self.port = port

        # read data from port

        self.lines = self.ssc.socketTextStream(self.IP, self.port)

    def __str__(self):
        pass

    def count_calltime(self, n):
        """
        This function is to read n lines data from redis, then count the call time duration sum of every hour.
        """

        # temp 2 block
        def helper(x):
            try:

                rds_type = redis.Redis(host="localhost", port=6379, decode_responses=True,
                                       db=1)  # host是redis主机，需要redis服务端和客户端都启动 redis默认端口是6379
                res = (x.split("|")[0], "Private") if rds_type.get(
                    x.split("|")[3]) is None else (
                    x.split("|")[0], rds_type.get(x.split("|")[3]))
                rds_type.close()
                return res

            except Exception as e:
                print(e)
                return x.split("|")[0], "Private"

        def mapper(x):

            tag = {
                # "Business",
                "Business": "Business",
                "Banking": "Business",
                "Financial agency": "Business",
                "Job": "Business",
                #  "Agency",
                "Legal agency": "Agency",
                "Housekeeping and property management": "Agency",
                #  "Education",
                "School": "Education",
                "Extracurricular training camp": "Education",
                # "Health",
                "Hospital (including health care)": "Health",
                "Clinic (including dentist)": "Health",
                # "AD",
                "Food (including takeaway)": "AD",
                "Dress code (booking and buying)": "AD",
                "Housing (including rental)": "AD",
                "Traveling": "AD",
                "Emergency": "Emergency",
                "Private": "Private"
            }
            return (x[0], "Private", x[2]) if tag.get(x[1]) == None else (x[0], tag.get(x[1]), x[2])

        process_lines = self.lines.map(helper)
        # process_lines.pprint()
        people_type_count = process_lines.countByValue().map(lambda x: (x[0][0], x[0][1], x[1]))
        # people_type_count.pprint()
        # First, people with type
        people_type_max = people_type_count.transform(
            lambda rdd: rdd.sortBy(lambda x: (x[0], -int(x[2]), x[1])).map(lambda x: (x[0], x[1])).reduceByKey(
                lambda x, y: x))

        # people_type_max.pprint()
        people_type_max.foreachRDD(lambda rdd: rdd.sortBy(lambda x: x[0]).toDF().toPandas().to_json(
            os.path.join(STORE_DIR, "tmp2", "pptype2.json")) if not rdd.isEmpty() else None)
        # Second, people with tag
        people_tag = people_type_count.map(mapper)
        people_tag_max = people_tag.transform(
            lambda rdd: rdd.sortBy(lambda x: (x[0], -int(x[2]), x[1])).map(lambda x: (x[0], x[1])).reduceByKey(
                lambda x, y: x))
        people_tag_max.pprint()
        people_tag_max.foreachRDD(lambda rdd: rdd.sortBy(lambda x: x[0]).toDF().toPandas().to_json(
            os.path.join(STORE_DIR, "tmp2", "pptag2.json")) if not rdd.isEmpty() else None)

        # temp 2

        # temp 5 block
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

        people_calltime_w_count.foreachRDD(
            lambda rdd: rdd.sortBy(lambda x: (x[0], -x[2], x[1])).map(lambda x: (x[0], x[1])).distinct().reduceByKey(
                lambda x, y: x)
            .sortBy(lambda x: x[0]).toDF().toPandas().to_json(
                os.path.join(STORE_DIR, "tmp5", "day2.json")) if not rdd.isEmpty() else None)
        people_calltime_w_count.pprint()
        people_calltime_d_count.foreachRDD(
            lambda rdd: rdd.sortBy(lambda x: (x[0], -x[2], x[1])).map(lambda x: (x[0], x[1])).distinct().reduceByKey(
                lambda x, y: x)
            .sortBy(lambda x: x[0]).toDF().toPandas().to_json(
                os.path.join(STORE_DIR, "tmp5", "clock2.json")) if not rdd.isEmpty() else None)
        people_calltime_d_count.pprint()

        # temp 5


def template_05_main():
    test_temp_5 = template_05(IP="localhost", port=9005)
    test_temp_5.count_calltime(None)

    test_temp_5.ssc.start()
    print("Start process 0 for template 5")
    time.sleep(60)
    # test_temp_0.ssc.stop(stopSparkContext=False, stopGraceFully=True)
    test_temp_5.ssc.awaitTermination()  # used for real time


if __name__ == '__main__':
    p5 = Process(target=template_05_main)
    p5.start()
    print("Wait for terminated")
    p5.join()