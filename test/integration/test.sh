#!/bin/bash
source /data/srv/current/apps/wmarchive/etc/profile.d/init.sh
cmd=myspark
echo "RecordCounter"
$cmd --yarn --spec=count.json --script=RecordCounter 2>&1 1>& count.log 
echo "RecordFinderFailures"
$cmd --yarn --spec=prepid.json --script=RecordFinderFailures --records-output=failures.records 2>&1 1>& failures.log
echo "RecordAggregator, should run without yarn"
$cmd --spec=empty_spec.json --script=RecordAggregator --hdir=hdfs:///cms/wmarchive/avro/2016/12/01/20161201_020812_051402.avro 2>&1 1>& agg.log
echo "LogFinder"
$cmd --yarn --script=LogFinder --spec=lf.json --records-output=lf.records 2>&1 1>& lf.log
echo "RecordFinderCMSRun"
$cmd --yarn --spec=prepid.json --script=RecordFinderCMSRun --records-output=prepid.records 2>&1 1>& prepid.log
echo "RecordFinder"
$cmd --yarn --spec=task.json --script=RecordFinder --records-output=task.records 2>&1 1>& task.log
echo "RecordFinderStorage, may run without yarn"
$cmd --spec=storage.json --script=RecordFinder --records-output=storage.records 2>&1 1>& storage.log
