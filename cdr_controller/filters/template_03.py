__author__ = "Jiajing Sun"
__email__ = "js5504@columbia.edu"

import redis
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from phonenumbers.phonenumberutil import region_code_for_country_code

rds_temp = redis.Redis(host='localhost', port=6379, decode_responses=True,
                       db=6)  # host是redis主机，需要redis服务端和客户端都启动 redis默认端口是6379


class template_3:
    """
    The third template to analyze the country of callednumber
    """
    default_connect_info = {
        'host': 'localhost',
        'user': 'root',
        'db': '0',
        'port': 6379
    }

    def __init__(self, IP="localhost", interval=10, port=6379):
        self.IP = IP
        self.interval = interval
        self.port = port

        # create spark context
        spark = SparkSession.builder.appName('myApp').getOrCreate()
        sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

        # create sql context, used for saving rdd
        sql_context = SparkSession(sc)

        # create the Streaming Context from the above spark context with batch interval size (seconds)
        ssc = StreamingContext(sc, self.interval)

        # read data from port
        self.lines = ssc.socketTextStream(self.IP, self.port)

    def __str__(self):
        pass

    def count_duration(self, n):
        """
        This function is to read n lines data from redis, then count the call time duration sum of every hour.
        """

        # Drop all invalid data. TODO

        def save_rdd(rdd):
            if not rdd.isEmpty():
                df = rdd.sortBy(lambda x: x[0]).toDF()
                # df.show()
                df.write.format("org.apache.spark.sql.redis").option("table", "counts").option("key.column", "_1").save(
                    mode='overwrite')

        callednumber = self.lines.map(lambda x: (x.split("|")[3]))
        place = callednumber.map(lambda x: region_code_for_country_code(int(x.split("-")[0].split("+")[1])))
        place_count = place.map(lambda place: (place, 1)).reduceByKey(lambda x, y: x + y)

        place_count.foreachRDD(save_rdd)


test_temp_3 = template_3()
test_temp_3.count_duration(None)
