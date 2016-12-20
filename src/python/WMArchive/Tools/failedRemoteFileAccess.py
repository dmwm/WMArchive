"""
Given a workflow, find rough precentage of remote access jobs and the failure rate.

August 8, 2016
Y. Guo 

"""

"""
The workflow name is the first part of task in fwjr.
task =/workflow/...
example of task: /vlimant_HIG-RunIISummer15wmLHEGS-00261_00062_v0_VBFPostMGFilter_160609_191625_1631/Production

"""
from pymongo import MongoClient
from bson.objectid import ObjectId
import re
muri = "mongodb://localhost:8230"
mclient = MongoClient(host=muri)
mdb = "Rfwjr"
mcol = "realData"
mgr = mclient[mdb][mcol]
k =0
workflow = "pdmvserv_task_SMP-RunIIFall15DR76-00083__v1_T_160313_135718_7429"
task_sr = re.compile("/" + workflow + "/*")

#calculate total number of localtion/file access for this workflow
c0 = mgr.find({"task":task_sr}, {"_id":1})
total = c0.count()

#calculate total number of remote location/file access for this workflow
totalR = 0
for j in c0:
    c3 = mgr.find({"_id": ObjectId(j["_id"]), "PFNArray": re.compile("root*")}, 
                  {"_id":1})
    if c3.count() > 0:              
        totalR = totalR + 1

c = mgr.find({"steps.errors.exitCode":8028, "task":task_sr}, {"_id":1})
totalFailed = c.count()
"""
for i in c:
    c2 = mgr.find({"_id": ObjectId(i["_id"]), "LFNArray": re.compile("root*")}, 
              {"_id":1})
    print(c2[0])          
    if c2.count() > 0:
        k = k + 1
"""
print("Total failed file: ", totalFailed)

#% remote acess:
print("% remote access", float(totalFailed+totalR)/total)

#% failed remote access
print("% failed remote access", float(totalFailed)/totalR)







