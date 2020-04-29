__author__ = "Wenjie Chen"
__email__ = "wc2685@columbia.edu"

import redis
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from multiprocessing import Process
import time


class template_0:
    """
    The first template to analyze CRD 
    """

    def __init__(self,IP="localhost",interval=10,port=9000):
        # create spark context
        self.spark = SparkSession.builder.appName('template0').getOrCreate()
        self.sc = SparkContext.getOrCreate(SparkConf().setMaster("local"))

        # create sql context, used for saving rdd
        self.sql_context = SparkSession(self.sc)

        # create the Streaming Context from the above spark context with batch interval size (seconds)
        self.ssc = StreamingContext(self.sc, 10)
        self.IP=IP
        self.interval=interval
        self.port=port

        # read data from port

        self.lines = self.ssc.socketTextStream(self.IP, self.port)


    def __str__(self):
        pass

    def count_duration(self,n):
        """
        This function is to read n lines data from redis, then count the call time duration sum of every hour.
        """
        
        id_time_duration = self.lines.map((lambda x: (x.split("|")[2], x.split("|")[4], x.split("|")[5])))

        # for i in range(24):
        #     # print("%d hour" % i)
        #     # temp_id_time_duration = id_time_duration.filter(lambda x: x[1].split(" ")[3].split(":")[0] == "%d" % i)
        temp_id_time_duration0 = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 0)
        temp_id_duration0 = temp_id_time_duration0.map(lambda x: ("%d" % 0, int(x[2])))
        temp_id_duration_total0 = temp_id_duration0.reduceByKey(lambda x, y: int(x) + int(y))
        # temp_id_duration_total0.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration1 = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 1)
        temp_id_duration1 = temp_id_time_duration1.map(lambda x: ("%d" % 1, int(x[2])))
        temp_id_duration_total1 = temp_id_duration1.reduceByKey(lambda x, y: int(x) + int(y))
        # temp_id_duration_total1.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration2 = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 2)
        temp_id_duration2 = temp_id_time_duration2.map(lambda x: ("%d" % 2, int(x[2])))
        temp_id_duration_total2 = temp_id_duration2.reduceByKey(lambda x, y: int(x) + int(y))
        # temp_id_duration_total2.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration3 = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 3)
        temp_id_duration3 = temp_id_time_duration3.map(lambda x: ("%d" % 3, int(x[2])))
        temp_id_duration_total3 = temp_id_duration3.reduceByKey(lambda x, y: int(x) + int(y))
        # temp_id_duration_total3.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration4 = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 4)
        temp_id_duration4 = temp_id_time_duration4.map(lambda x: ("%d" % 4, int(x[2])))
        temp_id_duration_total4 = temp_id_duration4.reduceByKey(lambda x, y: int(x) + int(y))
        # temp_id_duration_total4.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration5 = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 5)
        temp_id_duration5 = temp_id_time_duration5.map(lambda x: ("%d" % 5, int(x[2])))
        temp_id_duration_total5 = temp_id_duration5.reduceByKey(lambda x, y: int(x) + int(y))
        # temp_id_duration_total5.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration6 = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 6)
        temp_id_duration6 = temp_id_time_duration6.map(lambda x: ("%d" % 6, int(x[2])))
        temp_id_duration_total6 = temp_id_duration6.reduceByKey(lambda x, y: int(x) + int(y))
        # temp_id_duration_total6.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration7 = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 7)
        temp_id_duration7 = temp_id_time_duration7.map(lambda x: ("%d" % 7, int(x[2])))
        temp_id_duration_total7 = temp_id_duration7.reduceByKey(lambda x, y: int(x) + int(y))
        # temp_id_duration_total7.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration8 = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 8)
        temp_id_duration8 = temp_id_time_duration8.map(lambda x: ("%d" % 8, int(x[2])))
        temp_id_duration_total8 = temp_id_duration8.reduceByKey(lambda x, y: int(x) + int(y))
        # temp_id_duration_total8.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration9 = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 9)
        temp_id_duration9 = temp_id_time_duration9.map(lambda x: ("%d" % 9, int(x[2])))
        temp_id_duration_total9 = temp_id_duration9.reduceByKey(lambda x, y: int(x) + int(y))
        # temp_id_duration_total9.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration10 = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 10)
        temp_id_duration10 = temp_id_time_duration10.map(lambda x: ("%d" % 10, int(x[2])))
        temp_id_duration_total10 = temp_id_duration10.reduceByKey(lambda x, y: int(x) + int(y))
        # temp_id_duration_total10.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration11 = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 11)
        temp_id_duration11 = temp_id_time_duration11.map(lambda x: ("%d" % 11, int(x[2])))
        temp_id_duration_total11 = temp_id_duration11.reduceByKey(lambda x, y: int(x) + int(y))
        # temp_id_duration_total11.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration12 = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 12)
        temp_id_duration12 = temp_id_time_duration12.map(lambda x: ("%d" % 12, int(x[2])))
        temp_id_duration_total12 = temp_id_duration12.reduceByKey(lambda x, y: int(x) + int(y))
        # temp_id_duration_total12.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration13 = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 13)
        temp_id_duration13 = temp_id_time_duration13.map(lambda x: ("%d" % 13, int(x[2])))
        temp_id_duration_total13 = temp_id_duration13.reduceByKey(lambda x, y: int(x) + int(y))
        # temp_id_duration_total13.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration14 = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 14)
        temp_id_duration14 = temp_id_time_duration14.map(lambda x: ("%d" % 14, int(x[2])))
        temp_id_duration_total14 = temp_id_duration14.reduceByKey(lambda x, y: int(x) + int(y))
        # temp_id_duration_total14.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration15 = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 15)
        temp_id_duration15 = temp_id_time_duration15.map(lambda x: ("%d" % 15, int(x[2])))
        temp_id_duration_total15 = temp_id_duration15.reduceByKey(lambda x, y: int(x) + int(y))
        # temp_id_duration_total15.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration16 = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 16)
        temp_id_duration16 = temp_id_time_duration16.map(lambda x: ("%d" % 16, int(x[2])))
        temp_id_duration_total16 = temp_id_duration16.reduceByKey(lambda x, y: int(x) + int(y))
        # temp_id_duration_total16.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration17 = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 17)
        temp_id_duration17 = temp_id_time_duration17.map(lambda x: ("%d" % 17, int(x[2])))
        temp_id_duration_total17 = temp_id_duration17.reduceByKey(lambda x, y: int(x) + int(y))
        # temp_id_duration_total17.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration18 = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 18)
        temp_id_duration18 = temp_id_time_duration18.map(lambda x: ("%d" % 18, int(x[2])))
        temp_id_duration_total18 = temp_id_duration18.reduceByKey(lambda x, y: int(x) + int(y))
        # temp_id_duration_total18.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration19 = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 19)
        temp_id_duration19 = temp_id_time_duration19.map(lambda x: ("%d" % 19, int(x[2])))
        temp_id_duration_total19 = temp_id_duration19.reduceByKey(lambda x, y: int(x) + int(y))
        # temp_id_duration_total19.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration20 = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 20)
        temp_id_duration20 = temp_id_time_duration20.map(lambda x: ("%d" % 20, int(x[2])))
        temp_id_duration_total20 = temp_id_duration20.reduceByKey(lambda x, y: int(x) + int(y))
        # temp_id_duration_total20.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration21 = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 21)
        temp_id_duration21 = temp_id_time_duration21.map(lambda x: ("%d" % 21, int(x[2])))
        temp_id_duration_total21 = temp_id_duration21.reduceByKey(lambda x, y: int(x) + int(y))
        # temp_id_duration_total21.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration22 = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 22)
        temp_id_duration22= temp_id_time_duration22.map(lambda x: ("%d" % 22, int(x[2])))
        temp_id_duration_total22 = temp_id_duration22.reduceByKey(lambda x, y: int(x) + int(y))
        # temp_id_duration_total22.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        temp_id_time_duration23 = id_time_duration.filter(lambda x: int(x[1].split(" ")[3].split(":")[0]) == 23)
        temp_id_duration23 = temp_id_time_duration23.map(lambda x: ("%d" % 23, int(x[2])))
        temp_id_duration_total23 = temp_id_duration23.reduceByKey(lambda x, y: int(x) + int(y))
        # temp_id_duration_total23.pprint()
        # temp_id_duration_total.map(lambda x: int(x[1]))\
        #     .foreachRDD(lambda RDD: rds_temp.set("%d" % i,str(RDD.collect()[0])) if RDD.collect() else rds_temp.set("%d" % i, "0"))
        #


        temp_midnight_total=temp_id_duration_total0\
            .union(temp_id_duration_total1)\
            .union(temp_id_duration_total2)\
            .union(temp_id_duration_total3)\
            .union(temp_id_duration_total4)\
            .union(temp_id_duration_total5)

        temp_midnight_total.pprint()
        temp_midnight_total.foreachRDD(lambda rdd: rdd.sortBy(lambda x: x[0]).toDF().toPandas().to_json("../../res/tmp0/tmp0_midnight.json") if not rdd.isEmpty() else None)

        temp_morning_total=temp_id_duration_total6\
            .union(temp_id_duration_total7)\
            .union(temp_id_duration_total8)\
            .union(temp_id_duration_total9)\
            .union(temp_id_duration_total10)\
            .union(temp_id_duration_total11)

        temp_morning_total.pprint()
        temp_morning_total.foreachRDD(lambda rdd: rdd.sortBy(lambda x: x[0]).toDF().toPandas().to_json("../../res/tmp0/tmp0_morning.json") if not rdd.isEmpty() else None)

        temp_afternoon_total=temp_id_duration_total12\
            .union(temp_id_duration_total13)\
            .union(temp_id_duration_total14)\
            .union(temp_id_duration_total15)\
            .union(temp_id_duration_total16)\
            .union(temp_id_duration_total17)

        temp_afternoon_total.pprint()
        temp_afternoon_total.foreachRDD(lambda rdd: rdd.sortBy(lambda x: x[0]).toDF().toPandas().to_json("../../res/tmp0/tmp0_afternoon.json") if not rdd.isEmpty() else None)

        temp_evening_total=temp_id_duration_total18\
            .union(temp_id_duration_total19)\
            .union(temp_id_duration_total20)\
            .union(temp_id_duration_total21)\
            .union(temp_id_duration_total22)\
            .union(temp_id_duration_total23)

        temp_evening_total.pprint()
        temp_evening_total.foreachRDD(lambda rdd: rdd.sortBy(lambda x: x[0]).toDF().toPandas().to_json("../../res/tmp0/tmp0_evening.json") if not rdd.isEmpty() else None)


def template_0_main():
    test_temp_0=template_0(IP="localhost", port=9000)
    test_temp_0.count_duration(None)

    test_temp_0.ssc.start()
    print("Start process 0 for template 0")
    time.sleep(60)
    test_temp_0.ssc.stop(stopSparkContext=False, stopGraceFully=True)
    # test_temp_0.ssc.awaitTermination() # used for real time


if __name__ == '__main__':
    p0 = Process(target=template_0_main)
    p0.start()
    print("Wait for terminated")
    p0.join()
