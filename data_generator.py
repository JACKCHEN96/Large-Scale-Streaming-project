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


def gen_ID():
    return uuid.uuid1()

def gen_callnumber():
    callnumber=f1.create()
    return callnumber.phone_number()

def gen_callednumber():
    callednumber=f1.create()
    return callednumber.phone_number()

def gen_teltime():
    time=f1.create()
    starttime=time.iso8601(tzinfo=None)
    timedelta=random.randint(0,60*1000)
    return starttime+"|"+str(timedelta)

def gen_teltype():
    if(random.randint(0,1)==0):
        return "SMS"
    else:
        return "VOICE"

def gen_charge():
    return random.random()

def gen_result():
    r=random.randint(0,2)
    if(r==0):
        return "AWNSERED"
    elif (r==1):
        return "BUSY"
    else:
        return "VOICE"

# def gen_data(self):
#     return str(self.en_ID())+"|"+str(self.gen_callnumber())\
#            +str(self.gen_callednumber())+"|"+str(self.gen_teltime())\
#            +str(self.gen_teltype())+"|"+str(self.gen_charge())+"|"+str(self.gen_result())
def gen_data():
    return str(gen_ID())+"|"+str(gen_callnumber())+"|"\
           +str(gen_callednumber())+"|"+str(gen_teltime())+"|"\
           +str(gen_teltype())+"|"+str(gen_charge())+"|"+str(gen_result())

# print(gen_ID())
# print(gen_callnumber())
# print(gen_callednumber())
# print(gen_teltime())
# print(gen_teltype())
# print(gen_charge())
# print(gen_result())
print(gen_data())