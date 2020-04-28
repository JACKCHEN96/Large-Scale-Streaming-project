__author__ = "Wenjie Chen"
__email__ = "wc2685@columbia.edu"

import redis
import time
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

rds_temp = redis.Redis(host='localhost', port=6379, decode_responses=True,
                  db=7)  # host是redis主机，需要redis服务端和客户端都启动 redis默认端口是6379
rds_type = redis.Redis(host="localhost", port=6379, decode_responses=True,
                   db=1)  # host是redis主机，需要redis服务端和客户端都启动 redis默认端口是6379

# create spark context
spark = SparkSession.builder.appName('myApp').getOrCreate()
sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

# create sql context, used for saving rdd
sql_context = SparkSession(sc)

# create the Streaming Context from the above spark context with batch interval size (seconds)
ssc = StreamingContext(sc, 10)

class template_01:
    """
    The second template to analyze CRD. It reads CRD from the first redis DB and then extract the call type from the second redis DB.
    """

    def __init__(self,IP="localhost",interval=10,port=6379):
        self.IP=IP
        self.interval=interval
        self.port=port


        # read data from port
        self.lines = ssc.socketTextStream(self.IP, self.port)



    def __str__(self):
        pass

    def count_type(self,n):
        """
        This function is to read data and extract the call type.
        """

        # TODO. Drop all invalid data

        def save_rdd(rdd):
            if not rdd.isEmpty():
                df = rdd.sortBy(lambda x: x[0]).toDF()
                # df.show()
                df.write.format("org.apache.spark.sql.redis").option("table", "counts").option("key.column", "_1").save(mode='overwrite')

        process_lines=self.lines.map(lambda x: rds_type.get(x.split("|")[3]))

        resultRDD = (process_lines
                     .map(lambda word: word.lower())
                     .map(lambda word: (word, 1))  # 将word映射成(word,1)
                     .reduceByKey(lambda x, y: x + y))  # reduceByKey对所有有着相同key的items执行reduce操作


        resultRDD.foreachRDD(save_rdd)



test_temp_0=template_01()
test_temp_0.count_type(None)

ssc.start()
time.sleep(60)
ssc.stop(stopSparkContext=False, stopGraceFully=True)



