"""
Mapper and reducer methods for a MR job collecting average CPU time
spent per event (AvgEventCPU) on jobs for a given task name.
"""
import math

def mapper(ctx):
    "Read given context and yield task name and AvgEventCPU"
    rec = ctx.value
    try:
        taskname = rec['fwjr']['task']

        perf = rec['fwjr']['steps']['cmsRun1']['performance']
        avevcpu = float(perf['cpu']['AvgEventCPU'])
    except KeyError: # something not found
        avevcpu = None
        taskname = 'empty'
    except ValueError: # float() failed
        avevcpu = float('nan')
        taskname = 'empty'

    print 40*'#'
    print taskname
    print avevcpu
    print 40*'#'

    ctx.emit(taskname, avevcpu)

def reducer(ctx):
    "Emit taskname as key and calculate the average AvgEventCPU"

    total, avsum, nvalid = 0, 0.0, 0
    for avcpu in ctx.values:
        total += 1
        if not math.isnan(avcpu) and avcpu != None:
            avsum += avcpu
            nvalid += 1

    result = {
        'average' : avsum/nvalid,
        'failed'  : total-nvalid
    }

    print 40*'#'
    print result['average']
    print result['failed']
    print 40*'#'

    ctx.emit(ctx.key, result)
