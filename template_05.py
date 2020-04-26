__author__ = "Wenjie Chen"
__email__ = "wc2685@columbia.edu"

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

    def __init__(self, connect_info=None):

        self.connect_info = connect_info
        if connect_info == None:
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
            value_list.append(str(v))  # value_list

        spark = SparkSession.builder.appName('myApp').getOrCreate()
        sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

        self.lines = sc.parallelize(value_list)
        # print('show value list',value_list)

    def __str__(self):
        pass

    def count_calltime(self, n):
        """
        This function is to read n lines data from redis, then count the call time duration sum of every hour.
        """
        # Drop all invalid data. TODO
        people_calltime = self.lines.map(lambda x: (x.split("|")[0],x.split("|")[4]))

        people_calltime_w=people_calltime.map(lambda x:(x[0]+":"+x[1].split(" ")[0],1))
        people_calltime_d = people_calltime.map(lambda x: (x[0]+":"+x[1].split(" ")[3].split(":")[0],1))

        people_calltime_w_count=people_calltime_w.reduceByKey(lambda x, y: x + y).map(lambda x: (x[0].split(":")[0],x[0].split(":")[1],x[1]))
        people_calltime_d_count = people_calltime_d.reduceByKey(lambda x, y: x + y).map(lambda x: (x[0].split(":")[0],x[0].split(":")[1],x[1]))
        # print(people_calltime.take(20))
        # print(people_calltime_w_count.take(20))
        # print(people_calltime_d_count.take(20))

        people_w=people_calltime_w_count.sortBy(lambda x: (x[0],-x[2],x[1])).map(lambda x: (x[0],x[1])).distinct().reduceByKey(lambda x,y:x )
        people_d= people_calltime_d_count.sortBy(lambda x: (x[0],-x[2],x[1])).map(lambda x: (x[0],x[1])).distinct().reduceByKey(lambda x,y:x )

        print(people_w.take(20))
        print(people_d.take(20))


        # ptest=people_calltime.map(lambda x:(x[0],1)).reduceByKey(lambda x,y:x+y)
        # print(ptest.take(20))
        # ptest2=people_calltime_w_count.sortBy(lambda x: (x[0],-x[2],x[1]))
        # print(ptest2.take(50))

test_temp_3 = template_3()
test_temp_3.count_calltime(None)
