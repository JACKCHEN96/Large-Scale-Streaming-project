__author__ = "Wenjie Chen"
__email__ = "wc2685@columbia.edu"

import redis
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

rds_temp = redis.Redis(host='localhost', port=6379, decode_responses=True,
                  db=6)  # host是redis主机，需要redis服务端和客户端都启动 redis默认端口是6379

class template_0:
    """
    The first template to analyze CRD 
    """

    def __init__(self,IP="localhost",interval=10,port=6379):
        self.IP=IP
        self.interval=interval
        self.port=port

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

    def count_duration(self,n):
        """
        This function is to read n lines data from redis, then count the call time duration sum of every hour.
        """
        
        id_time_duration = self.lines.map(lambda x: (x.split("|")[2], x.split("|")[4], x.split("|")[5]))

        for i in range(24):
            print("%d hour" % i)
            temp_id_time_duration = id_time_duration.filter(lambda x: x[1].split(" ")[3].split(":")[0] == "%d" % i)
            temp_id_duration = temp_id_time_duration.map(lambda x: ("%d" % i, int(x[2])))
            temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))

            temp_id_duration_total.map(lambda x: int(x[1]))\
                .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))

            # print(temp_id_duration_total.map(lambda x: int(x[1])).collect())
            # print(temp_id_duration_total.map(lambda x: int(x[1])))
            # if temp_id_duration_total.map(lambda x: int(x[1])).collect():
            #     rds_temp.set("%d" % i, str(temp_id_duration_total.map(lambda x: int(x[1])).collect()[0]))
            # else:
            #     rds_temp.set("%d" % i, "0")




test_temp_0=template_0()
test_temp_0.count_duration(None)

