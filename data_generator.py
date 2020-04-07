__author__ = "Wenjie Chen"
__email__ = "wc2685@columbia.edu"

# from faker import Faker
import faker
import random
import datetime
import uuid

# f1=faker.Faker().address()
# print(f1)
f1=faker.Factory

# for i in range (100):

class data_generator:
    """
    A complete CRD
    """
    def __init__(self,ID=None):
        # self.ID=ID
        pass

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

    def gen_data(self):
        # self.ID=self.gen_ID()
        return str(self.gen_ID())+"|"+str(self.gen_callnumber())+"|"\
               +str(self.gen_callednumber())+"|"+str(self.gen_teltime())+"|"\
               +str(self.gen_teltype())+"|"+str(self.gen_charge())+"|"+str(self.gen_result())

d1=data_generator()
d2=data_generator()
print(d1.gen_data())
print(d2.gen_data())