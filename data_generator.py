__author__ = "Wenjie Chen"
__email__ = "wc2685@columbia.edu"

# from faker import Faker
import faker
import random
import datetime
import uuid

f1=faker.Factory

class data_generator:
    """
    A complete CRD
    """
    def __init__(self,ID=None,callnumber=None,callednumber=None,teltime=None,teltype=None,charge=None,result=None):
        self.ID=self.gen_ID()
        self.callnumber=self.gen_callnumber()
        self.callednumber=self.gen_callednumber()
        self.teltime=self.gen_teltime()
        self.teltype=self.gen_teltype()
        self.charge=self.gen_charge()
        self.result=self.gen_result()
        if ID is not None: self.ID=ID
        if callnumber is not None: self.callnumber = callnumber
        if callednumber is not None: self.callednumber = callednumber
        if teltime is not None: self.telltime = teltime
        if teltype is not None: self.teltype = teltype
        if charge is not None: self.charge = charge
        if result is not None: self.result = result

    def __str__(self):
        data = str(self.ID)+"|"+str(self.callnumber)+"|"\
               +str(self.callednumber)+"|"+str(self.teltime)+"|"\
               +str(self.teltype)+"|"+str(self.charge)+"|"+str(self.result)
        return data

    def gen_ID(self):
        return uuid.uuid1()

    def gen_callnumber(self):
        callnumber=f1.create()
        return callnumber.phone_number()

    def gen_callednumber(self):
        callednumber=f1.create()
        return callednumber.phone_number()

    def gen_teltime(self):
        time=f1.create()
        starttime=time.iso8601(tzinfo=None)
        timedelta=random.randint(0,60*1000)
        return starttime+"|"+str(timedelta)

    def gen_teltype(self):
        if(random.randint(0,1)==0):
            return "SMS"
        else:
            return "VOICE"

    def gen_charge(self):
        return random.random()

    def gen_result(self):
        r=random.randint(0,2)
        if(r==0):
            return "AWNSERED"
        elif (r==1):
            return "BUSY"
        else:
            return "VOICE"

    def output_redis(self):
        pass

d1=data_generator(ID=1)
d2=data_generator()
print(d1.ID)
print(d1)
print(d2)