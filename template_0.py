__author__ = "Wenjie Chen"
__email__ = "wc2685@columbia.edu"

import redis
import numpy as np
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

class template_0:
    """
    The first template to analyze CRD
    """
    default_connect_info = {
        'host': 'localhost',
        'user': 'root',
        'db': '0',
        'port': 6379
    }

    def __init__(self,connect_info=None):
        self.connect_info=connect_info
        if connect_info== None:
            self.connect_info = self.default_connect_info

        rds = redis.Redis(host=self.connect_info['host'],
                          port=self.connect_info['port'],
                          decode_responses=True,
                          db=self.connect_info['db']
                          )  # host是redis主机，需要redis服务端和客户端都启动 redis默认端口是6379


    def __str__(self):
        pass

    def count_duration(self,n):
        """
        This function is to read n lines data from redis, then count the call time duration sum of every hour.
        """
        spark = SparkSession.builder.appName('myApp').getOrCreate()
        sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
        lines = sc.textFile("test_db_0.txt")
        id_time_duration = lines.map(lambda x: (x.split("|")[2], x.split("|")[4], x.split("|")[5]))

        # TODO. Drop all invalid data
        # id_time_duration_np=ip_time_duration_np.filter((lambda x: x[0].replace(":","").isdigit()))
        # id_time_duration=id_time_duration_np.filter(lambda x: x[-1].isdigit())

        print("Id_time_duration for all")
        print(id_time_duration.take(70))
        print("\n")
        test_id_time_duration = id_time_duration.filter(lambda x: x[1].split("T")[1].split(":")[0] == '14')
        test_id_duration = test_id_time_duration.map(lambda x: ("0", x[2]))
        test_total = test_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
        print("0 'clock")
        print(test_id_duration.take(70))
        print("Total time")
        print(test_total.take(50))
        print("\n")
        #
        print("******************************************")
        print(id_time_duration.take(20))
        print("\n")
        #
        for i in range(24):
            print("%d hour" % i)
            temp_id_time_duration = id_time_duration.filter(lambda x: x[1].split("T")[1].split(":")[0] == "%d" % i)
            temp_id_duration = temp_id_time_duration.map(lambda x: ("%d" % i, int(x[2])))
            temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
            print(temp_id_duration_total.take(20))
            print("\n")



test_temp_0=template_0()
test_temp_0.count_duration(None)



