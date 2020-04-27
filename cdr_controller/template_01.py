__author__ = "Wenjie Chen"
__email__ = "wc2685@columbia.edu"

import redis
import numpy as np
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

class template_01:
    """
    The second template to analyze CRD. It reads CRD from the first redis DB and then extract the call type from the second redis DB.
    """
    default_connect_info = {
        'host': 'localhost',
        'user': 'root',
        'db': 0,
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
        rds1 = redis.Redis(host=self.connect_info['host'],
                          port=self.connect_info['port'],
                          decode_responses=True,
                          db=1
                          )  # host是redis主机，需要redis服务端和客户端都启动 redis默认端口是6379

        pipe = rds.pipeline()
        pipe_size = 100000
        len = 0
        key_list = []
        number_list = []
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
            number_list.append(str(v))

        value_list=[]
        for number in number_list:
            value_list.append(rds1.get(number.split("|")[3]))

        spark = SparkSession.builder.appName('myApp').getOrCreate()
        sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

        # self.lines = sc.textFile("test_db_0.txt")
        self.lines = sc.parallelize(value_list)

    def __str__(self):
        pass

    def count_type(self,n):
        """
        This function is to read data and extract the call type.
        """

        # TODO. Drop all invalid data
        # id_time_duration_np=ip_time_duration_np.filter((lambda x: x[0].replace(":","").isdigit()))
        # id_time_duration=id_time_duration_np.filter(lambda x: x[-1].isdigit())
        resultRDD = (self.lines
                     .map(lambda word: word.lower())
                     .map(lambda word: (word, 1))  # 将word映射成(word,1)
                     .reduceByKey(lambda x, y: x + y))  # reduceByKey对所有有着相同key的items执行reduce操作
        # print(resultRDD.collect())
        print(resultRDD.sortBy(lambda x: x[1], ascending=False).take(20))



test_temp_0=template_01()
test_temp_0.count_type(None)



