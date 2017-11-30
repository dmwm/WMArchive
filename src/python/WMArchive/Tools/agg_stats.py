#!/usr/bin/env python
import json
import argparse
from datetime import datetime, timedelta
from pymongo import MongoClient

class OptionParser(object):
    "User based option parser"
    def __init__(self):
        self.parser = argparse.ArgumentParser(prog='mongo2hdfs')
        uri = 'mongodb://localhost:8230'
        self.parser.add_argument("--mongo", action="store",\
            dest="muri", default=uri, help="MongoDB URI, default %s" % uri)
        today = datetime.today().strftime('%Y%m%d')
        self.parser.add_argument("--date", action="store",\
            dest="date", default=today, help="date (YYYYMMDD), default today")
        dbname = 'aggregated.fwjr'
        self.parser.add_argument("--dbname", action="store",\
                dest="dbname", default=dbname, \
                help="aggregated dbname, default {}".format(dbname))

def main():
    "Main function"
    optmgr = OptionParser()
    opts = optmgr.parser.parse_args()

    client = MongoClient(opts.muri)
    dbname, dbcoll = 'aggregated', 'fwjr'
    if opts.dbname:
        dbname, dbcoll = opts.dbname.split('.')
    coll = client[dbname][dbcoll]

    date = datetime.strptime(opts.date, '%Y%m%d')
    spec = {"scope.start_date": date}
    workflows = set()
    tasks = set()
    task_names = set()
    events = 0
    for row in coll.find(spec):
	scope = row['scope']
	workflow = scope['workflow']
	task = scope['task']
	task_name = '/%s/%s' % (workflow, task)
	workflows.add(workflow)
	tasks.add(task)
	task_names.add(task_name)
	evts = row.get('events', 0)
	if  evts == None or evts == 'None':
	    evts = 0
	events += int(evts)
    print("Stats %s" % date.isoformat())
    print("# records   : %s" % coll.find(spec).count())
    print("# workflows : %s" % len(workflows))
    print("# tasks     : %s" % len(tasks))
    print("# task_names: %s" % len(task_names))
    print("# events    : %s" % events)
    print("\nOne record:")
    rec = coll.find_one(spec)
    del rec['_id']
    rec['scope']['start_date'] = rec['scope']['start_date'].isoformat()
    rec['scope']['end_date'] = rec['scope']['end_date'].isoformat()
    print(json.dumps(rec))

    sdate = date
    edate = sdate + timedelta(days=1)

    # jobstate query, here we print exact query as it appears in
    # src/python/WMArchive/Storate/MongoIO.py module, see performance function
    query = [{'$match': {'scope.start_date': {'$gte': sdate}}}, {'$match': {'scope.end_date': {'$lte': edate}}}, {'$group': {'count': {'$sum': '$count'}, '_id': {'jobstate': '$scope.jobstate', 'axis': {'start_date': '$scope.start_date', 'end_date': '$scope.end_date'}}}}, {'$group': {'_id': '$_id.axis', 'jobstates': {'$push': {'count': '$count', 'jobstate': '$_id.jobstate'}}}}, {'$project': {'_id': False, 'jobstates': '$jobstates', 'label': {'start_date': {'$dateToString': {'date': '$_id.start_date', 'format': '%Y-%m-%dT%H:%M:%S.%LZ'}}, 'end_date': {'$dateToString': {'date': '$_id.end_date', 'format': '%Y-%m-%dT%H:%M:%S.%LZ'}}}}}]
    res = coll.aggregate(query)
    print("\nAggregated query")
    print(query)
    print("\nAggregated content")
    print(json.dumps(res))

    # we simplify query to a matching content to verify that aggregation works as expected
    print("\nManual counting")
    query = [{'$match': {'scope.start_date': {'$gte': sdate}}}, {'$match': {'scope.end_date': {'$lte': edate}}}]
    tot = 0
    states = ['success', 'jobfailed', 'submitfailed']
    slen = max([len(s) for s in states])
    for state in states:
	spec = query + [{'$match': {'scope.jobstate':state}}]
	res = coll.aggregate(spec)
	count = 0
	for idx, rec in enumerate(res['result']):
	    count += int(rec['count'])
        pad = ' '*(slen-len(state))
        print('%s%s: %s' % (state, pad, count))
	tot += count
    print('%s%s: %s' % ('total', ' '*(slen-len('total')), tot))

if __name__ == '__main__':
    main()
