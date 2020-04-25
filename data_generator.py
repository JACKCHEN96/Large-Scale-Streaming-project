__author__ = "Wenjie Chen"
__email__ = "wc2685@columbia.edu"

# from faker import Faker
import faker
import random
import datetime
import uuid
import redis
import numpy as np
from phonenumbers.phonenumberutil import region_code_for_country_code

f1=faker.Factory
rds = redis.Redis(host='localhost', port=6379, decode_responses=True, db=0)   # host是redis主机，需要redis服务端和客户端都启动 redis默认端口是6379
rds_type = redis.Redis(host='localhost', port=6379, decode_responses=True, db=1)
rds_type_2 =  redis.Redis(host='localhost', port=6379, decode_responses=True, db=2)


class data_generator:
    """
    A complete CRD
    """
    def __init__(self,ID=None,callednumber=None,teltime=None,teltype=None,charge=None,result=None,type=None,
                 pick_type_distribution="default", rate_type_distribution=0.1,
                 pick_call_distribution="default", delta_distribution="default",
                 rate_place_distribution=0.7):

        self.ID=self.gen_ID()
        self.callednumber=self.gen_callednumber(rate_place_distribution)
        self.teltime=self.gen_teltime(pick_call_distribution,delta_distribution)
        self.teltype=self.gen_teltype()
        self.charge=self.gen_charge()
        self.result=self.gen_result()
        self.type=self.gen_type(pick_type_distribution, rate_type_distribution)

        if ID is not None: self.ID=ID
        if callednumber is not None: self.callednumber = callednumber
        if teltime is not None: self.telltime = teltime
        if teltype is not None: self.teltype = teltype
        if charge is not None: self.charge = charge
        if result is not None: self.result = result
        if type is not None: self.type=type
        self.output_redis_2()

    def __str__(self):
        data = str(self.ID)+"|"\
               +str(self.callednumber)+"|"+str(self.teltime)+"|"\
               +str(self.teltype)+"|"+str(self.charge)+"|"+str(self.result)
        return data

    def gen_ID(self):
        return uuid.uuid1()

    def gen_callednumber(self, rate_place_distribution):
        r = random.randint(1,100)
        if r<100*rate_place_distribution:
            place="1"
        else:
            place = random.randint(1, 300)
            while(region_code_for_country_code(place)=="ZZ"):
                place=random.randint(1,300)

        first = str(random.randint(100, 999))
        second = str(random.randint(1, 888)).zfill(3)
        last = str(random.randint(1, 9998)).zfill(4)
        return '+{}-{}-{}-{}'.format(str(place), first, second, last)

    def gen_teltime(self,pick_call_distribution,delta_distribution):
        """
        A dummy way to do this, to be continued.
        """
        r=random.randint(0,6)
        i=1
        if(r==0):
            call_distribution="midnight mode"
        elif (r==1):
            call_distribution="morning mode"
        elif (r==2):
            call_distribution="afternoon mode"
        elif (r==3):
            call_distribution="evening mode"
        else:
            call_distribution=pick_call_distribution

        time=f1.create()
        if(call_distribution=="midnight mode"):
            # TODO
            starttime = time.iso8601(tzinfo=None)
            while(starttime.split("T")[1].split(":")[0]>"6" and i<4):
                starttime = time.iso8601(tzinfo=None)
                i+=1
        elif(call_distribution=="morning mode"):
            # TODO
            starttime = time.iso8601(tzinfo=None)
            while(starttime.split("T")[1].split(":")[0]>"12" or starttime.split("T")[1].split(":")[0]<"6" and i<4):
                starttime = time.iso8601(tzinfo=None)
                i+=1
        elif(call_distribution=="afternoon mode"):
            starttime = time.iso8601(tzinfo=None)
            while(starttime.split("T")[1].split(":")[0]>"18" or starttime.split("T")[1].split(":")[0]<"12" and i<4):
                starttime = time.iso8601(tzinfo=None)
                i+=1
        elif(call_distribution=="evening mode"):
            starttime = time.iso8601(tzinfo=None)
            while(starttime.split("T")[1].split(":")[0]<"18" and i<4):
                starttime = time.iso8601(tzinfo=None)
                i+=1
        else:
            starttime = time.iso8601(tzinfo=None)

        # If you are confused of distribution command, please refer to this tutorial:
        # https://blog.csdn.net/howhigh/article/details/78007317
        if(delta_distribution=="exponential"):
            # TODO
            timedelta = np.random.exponential(600, 1)[0]
        elif(delta_distribution=="poisson"):
            # TODO
            timedelta = np.random.poisson(600, 1)[0]
        elif(delta_distribution=="binomial"):
            # TODO
            timedelta = np.random.binomial(1200,0.5,1)[0]
        else:
            timedelta = random.randint(0, 6000)
            # timedelta = np.random.exponential(600, 1)[0]
            # timedelta = np.random.poisson(600, 1)[0]
            # timedelta = np.random.binomial(1200, 0.5, 1)[0]

        return starttime+"|"+str(timedelta)

    def gen_teltype(self):
        if(random.randint(0,1)==0):
            return "SMS"
        else:
            return "VOICE"

    def gen_charge(self):
        return random.random()

    def gen_result(self):
        r=random.randint(1,10)
        if(r<9):
            return "AWNSERED"
        else:
            return "Busy"

    def gen_type(self, pick_type_distribution, rate_type_distribution):
        type={
            0: "Business",
            1: "Agency",
            2: "Education",
            3: "Scam",
            4: "Health",
            5: "AD"
        }
        r = random.randint(0, 5)
        if(pick_type_distribution=="default"):
            return type.get(r)

        r2=random.randint(1,100)
        if(r2<100*rate_type_distribution):
            return pick_type_distribution
        # r=(6N-1)/5
        return type.get(r)

    def output_redis_2(self):
        rds_type.set(str(self.callednumber),str(self.type))


class people:
    """
    A unique people with 1-5 telephones makes several calls
    """
    def __init__(self,ID=None,callnumber=None,calltimes=None,
                 callednumber=None, teltime=None, teltype=None, charge=None, result=None, type=None,
                 pick_type_distribution="default", rate_type_distribution=0.3,
                 pick_call_distribution="default", delta_distribution="default",
                 rate_place_distribution=0.7
                 ):
        # People
        self.ID=self.gen_ID()
        self.callnumber=self.gen_callnumber()
        self.calltimes=self.gen_calltimes()
        if ID is not None: self.ID=ID
        if callnumber is not None: self.callnumber = callnumber
        if calltimes is not None: self.calltimes=calltimes

        self.data=[]
        self.gen_data(None, callednumber, teltime, teltype, charge, result, type,
                      pick_type_distribution, rate_type_distribution,
                      pick_call_distribution, delta_distribution,
                      rate_place_distribution)


    def __str__(self):
        fuldata=""
        for i in range(self.calltimes):
            tempdata=str(self.ID)+"|"+str(self.callnumber)+"|"\
                     +(str(self.data[i]))
            self.output_redis_1(self.data[i].ID,tempdata)
            fuldata+=tempdata+"\n"
        return fuldata

    def gen_ID(self):
        return uuid.uuid1()

    def gen_callnumber(self):
        r = random.randint(1, 10)
        if r<8:
            place="1"
        else:
            place = random.randint(1, 300)
            while(region_code_for_country_code(place)=="ZZ"):
                place=random.randint(1,300)
        first = str(random.randint(100, 999))
        second = str(random.randint(1, 888)).zfill(3)
        last = str(random.randint(1, 9998)).zfill(4)

        return '+{}-{}-{}-{}'.format(str(place), first, second, last)

    def gen_calltimes(self):
        return random.randint(1,5)

    def gen_data(self,ID= None, callednumber=None, teltime=None, teltype=None, charge=None, result=None, type=None,
                 pick_type_distribution="default", rate_type_distribution=0.3,
                 pick_call_distribution="default", delta_distribution="default",
                 rate_place_distribution=0.7):
        for i in range(self.calltimes):
            data_generator_temp=data_generator(ID, callednumber, teltime, teltype, charge, result, type,
                                               pick_type_distribution, rate_type_distribution,
                                               pick_call_distribution, delta_distribution,
                                               rate_place_distribution)
            self.data.append(data_generator_temp)

    def output_redis_1(self,ID, tempdata):
        rds.set(str(ID),str(tempdata))


# test generate
print("----------- test generate -----------")
d1 = data_generator(ID=1)
d2 = data_generator(pick_call_distribution="morning mode",pick_type_distribution="Education",rate_type_distribution=10)
print(d1)
print(d2)
print(d2.type)

# test people
print("------------ test people ------------")
p1 = people()
print(p1)

# # test iterate redis datas
# print("-------------- test iterate redis data --------------")
# keys = rds.keys()
# print(keys)
#
# # test iterate redis_type data
# print("-------------- test iterate redis_type data --------------")
# keys_type = rds_type.keys()
# print(keys_type)

