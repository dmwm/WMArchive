"""
Given a workflow, find performance numbers, such as total job time, total cpu time 
(this is very general... but could be things like time per event, 
size per event, memory used, cores, etc)

July 29, 2016
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

workflow = "pdmvserv_task_SMP-RunIIFall15DR76-00083__v1_T_160313_135718_7429"
task_sr = re.compile("/" + workflow + "/*")


mgr.aggregate([{$match: {"task": task_sr}}, { $group: {_id:null, totalJobCPU: { $sum:"$steps.performance.cpu.TotalJobCPU"}, totalJobTime:{$sum: "$steps.performance.cpu.TotalJobTime"}, count: {$sum:1}}}])

totalCount = 0
c = mgr.aggregate([ 
                    {"$match": {"task":task_sr}}, 
                    {"$unwind": "$steps"}, 
                    {"$project": {"steps.performance.cpu":1} }, 
                    {"$match": {"steps.performance.cpu":{"$exists" : True, "$ne": {}} }}, 
                    { "$group": { "_id": None, "count": { "$sum": 1 } } }
                    ])

totalCount  = c['result'][0]['count'] 

c2 = mgr.aggregate([
                    {"$match": {"task": task_sr}},
                    {"$unwind": "$steps"}, 
                    {"$group": {"_id" : None, "totalJobCPU": { "$sum" : "$steps.performance.cpu.TotalJobCPU"},
                               "EventThroughput" : {"$sum": "$steps.performance.cpu.EventThroughput"},
                               "AvgEventTime" : {"$sum": "$steps.performance.cpu.AvgEventTime"},
                               "MaxEventTime" : {"$sum": "$steps.performance.cpu.MaxEventTime"},
                               "TotalLoopCPU" : {"$sum": "$steps.performance.cpu.TotalLoopCPU"},
                               "MinEventTime" : {"$sum": "$steps.performance.cpu.MinEventTime"},
                               "totalJobTime" : {"$sum" : "$steps.performance.cpu.TotalJobTime"}, 
                               "count": {"$sum" :1}}}
                  ])
"""
These averages are over entire workflow. We cannot use the count in c2 to calculate the average 
because we have a lot of empty fields in order to fullfill the schema requirement. These empty fields will be counted
into the count of c2
"""
AveTotalJobCPU = c2["result"][0]["totalJobCPU"]/totalCount
AveEventThroughput = c2["result"][0]["EventThroughput"]/totalCount
AveMaxEventTime = c2["result"][0]["MaxEventTime"]/totalCount
AveEventTime = c2["result"][0]["AvgEventTime"]/totalCount
AveTotalLoopCPU = c2["result"][0]["TotalLoopCPU"]/totalCount
AveMinEventTime = c2["result"][0]["MinEventTime"]/totalCount
AveTotalJobTime = c2["result"][0]["totalJobTime"]/totalCount










