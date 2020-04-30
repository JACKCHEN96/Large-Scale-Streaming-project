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
    The third template to analyze the type/tag of people
    """

    def __init__(self, IP="localhost", interval=10, port=9002):
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
        This function is to analyze people tag.
        For example, Bob call: Housekeeping and property management *10, Banking *8, Financial agency *5. Then he is a business tag man(13), hpm type man(10).
        """

        # Drop all invalid data. TODO

        def helper(x):
            rds_type = redis.Redis(host="localhost", port=6379, decode_responses=True,
                                   db=1)  # host是redis主机，需要redis服务端和客户端都启动 redis默认端口是6379

            return (x.split("|")[0],"Private") if rds_type.get(x.split("|")[3])==None else (x.split("|")[0],rds_type.get(x.split("|")[3]))

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
            return (x[0],"Private",x[2]) if tag.get(x[1])==None else (x[0], tag.get(x[1]), x[2])


        process_lines=self.lines.map(helper)
        # process_lines.pprint()
        people_type_count=process_lines.countByValue().map(lambda x: (x[0][0],x[0][1],x[1]))
        people_type_count.pprint()
        # First, people with type
        people_type_max=people_type_count.transform(lambda rdd: rdd.sortBy(lambda x: (x[0],-int(x[2]),x[1])).map(lambda x: (x[0],x[1])).reduceByKey(lambda x,y:x))

        people_type_max.pprint()
        people_type_max.foreachRDD(lambda rdd: rdd.sortBy(lambda x: x[0]).toDF().toPandas().to_json(os.path.join(STORE_DIR, "tmp2", "pptype.json")) if not rdd.isEmpty() else None)
        # Second, people with tag
        people_tag=people_type_count.map(mapper)
        people_tag_max=people_tag.transform(lambda rdd: rdd.sortBy(lambda x: (x[0],-int(x[2]),x[1])).map(lambda x: (x[0],x[1])).reduceByKey(lambda x,y:x))

        people_type_max.foreachRDD(lambda rdd: rdd.sortBy(lambda x: x[0]).toDF().toPandas().to_json(os.path.join(STORE_DIR, "tmp2", "pptag.json")) if not rdd.isEmpty() else None)
        people_tag_max.pprint()



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