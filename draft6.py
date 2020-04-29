import redis
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import time

rds_temp = redis.Redis(host='localhost', port=6379, decode_responses=True,
                  db=6)  # host是redis主机，需要redis服务端和客户端都启动 redis默认端口是6379

# create spark context
spark = SparkSession.builder.appName('myApp').getOrCreate()
sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

# create sql context, used for saving rdd
sql_context = SparkSession(sc)

# create the Streaming Context from the above spark context with batch interval size (seconds)
ssc = StreamingContext(sc, 5)

lines = ssc.socketTextStream("localhost", 9000)

# lines.pprint()
# lines.foreachRDD(lambda RDD: print(RDD.collect()))

id_time_duration = lines.map((lambda x: (x.split("|")[2], x.split("|")[4], x.split("|")[5])))
# id_time_duration = lines.flatMap((lambda x: (x.split("|")[2], x.split("|")[4], x.split("|")[5])))

# 111111111
# id_time_duration.pprint()

# 222222222


# temp_id_time_duration = id_time_duration.filter(lambda x: 0 if len(x)<2 else int(x[1].split(" ")[3].split(":")[0]) == 1)
# # temp_id_time_duration.pprint()
# # ----------------------------------------------------------------------------
# temp_id_duration = temp_id_time_duration.map(lambda x: (0, int(x[2])))
# temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
# def save_rdd(rdd):
#     if not rdd.isEmpty():
#         df = rdd.sortBy(lambda x: x[0]).toDF()
#         # df.show()
#         df.write.format("org.apache.spark.sql.redis").option("table", "counts").option("key.column", "_1").save(
#             mode='overwrite')

i=1
temp_id_time_duration = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == i)

temp_id_duration = temp_id_time_duration.map(lambda x: ("%d" % i, int(x[2])))
temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))

temp_id_duration_total.pprint()

# temp_id_duration_total.map(lambda x: int(x[1]))\
#     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))

# temp_id_duration_total.foreachRDD(save_rdd)

# ////////////

ssc.start()
time.sleep(60)
ssc.stop(stopSparkContext=False, stopGraceFully=True)
