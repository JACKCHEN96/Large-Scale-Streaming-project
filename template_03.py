__author__ = "Jiajing Sun"
__email__ = "js5504@columbia.edu"

import redis
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from phonenumbers.phonenumberutil import region_code_for_country_code

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

    def __init__(self,connect_info=None):

        self.connect_info=connect_info
        if connect_info== None:
            self.connect_info = self.default_connect_info

        rds = redis.Redis(host=self.connect_info['host'],
                          port=self.connect_info['port'],
                          decode_responses=True,
                          db=self.connect_info['db']
                          )  # host是redis主机，需要redis服务端和客户端都启动 redis默认端口是6379


        pipe = rds.pipeline()
        pipe_size = 100000
        len = 0
        key_list = []
        value_list = []
        # print(r.pipeline())
        keys = rds.keys()
        for key in keys:
            key_list.append(key)
            pipe.get(key)
            if len < pipe_size:
                len += 1
            else:
                for (k, v) in zip(key_list, pipe.execute()):
                    print(k, v)
                len = 0
                key_list = []

        for (k, v) in zip(key_list, pipe.execute()):
            value_list.append(str(v))                            # value_list

        spark = SparkSession.builder.appName('myApp').getOrCreate()
        sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))


        self.lines = sc.parallelize(value_list)
        # print('show value list',value_list)

    def __str__(self):
        pass

    def count_duration(self,n):
        """
        This function is to read n lines data from redis, then count the call time duration sum of every hour.
        """
        # Drop all invalid data. TODO

        callednumber = self.lines.map(lambda x: (x.split("|")[3]))
        place=callednumber.map(lambda x: region_code_for_country_code(int(x.split("-")[0].split("+")[1])))
        place_count=place.map(lambda place: (place, 1)).reduceByKey(lambda x, y: x + y)
        print(place_count.take(20))




        
test_temp_3 = template_3()
test_temp_3.count_duration(None)
