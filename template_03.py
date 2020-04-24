__author__ = "Jiajing Sun"
__email__ = "js5504@columbia.edu"

import redis
import numpy as np
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import findspark

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
        # Drop all invalid data
        #id_time_duration_np=ip_time_duration_np.filter((lambda x: x[0].replace(":","").isdigit()))
        # id_time_duration=id_time_duration_np.filter(lambda x: x[-1].isdigit())

        id_time_duration = self.lines.map(lambda x: (x.split("|")[3]))

        print("Total number",len(id_time_duration.take(120)))
        print(id_time_duration.take(120))
        print("\n")

        # Select the first digits of the callednumber
        temp_id_time_duration = id_time_duration.map(lambda x: (x.replace('.','-').split('-')[0]))
        # Filter the first digits with only numerical value 
        temp_filter_digit = temp_id_time_duration.filter(lambda x: x[0].isdigit())
        
        # Filter the number with "+"
        temp_filter_plus = temp_id_time_duration.filter(lambda x: x[0] == "+")
        # Filter the number with "("
        temp_filter_bracket = temp_id_time_duration.filter(lambda x: x[0] == "(")

        # print all the first digits
        print(temp_id_time_duration.take(120))
        print('Total_Callednumber',len(temp_id_time_duration.take(120)))
        print("\n")
        # print the first figits with only numerical value 
        print(temp_filter_digit.take(100))
        print('China',len(temp_filter_digit.take(100)))  # number with only digit 
        print("\n")
        # print the first digits with "+1"
        print(temp_filter_plus.take(100))
        print('United States',len(temp_filter_plus.take(100))) 
        print("\n")
        # print the first digits with "()"
        print(temp_filter_bracket.take(100))
        print('Canada',len(temp_filter_bracket.take(100))) 
        print("\n")

        
test_temp_3 = template_3()
test_temp_3.count_duration(None)
