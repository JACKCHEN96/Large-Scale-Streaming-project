import redis
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import time

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
temp_id_time_duration = id_time_duration.filter(lambda x: 0 if len(x)<2 else x[1].split(" ")[3].split(":")[0] == "01")
# temp_id_time_duration.pprint()


# ----------------------------------------------------------------------------
temp_id_duration = temp_id_time_duration.map(lambda x: (0, int(x[2])))
temp_id_duration_total = temp_id_duration.reduceByKey(lambda x, y: int(x) + int(y))
#
#
temp_id_duration_total.pprint()

ssc.start()
time.sleep(60)
ssc.stop(stopSparkContext=False, stopGraceFully=True)
