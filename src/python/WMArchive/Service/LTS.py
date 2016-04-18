#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : LTSManager.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: WMArchive Long-Term Storage manager. This module is responsible
for providing read/write APIs to Long-Term Storage.
"""

# futures
from __future__ import print_function, division

# system modules
import os
import json
import subprocess

# avro modules
import avro.schema
import avro.io

# hdfs pydoop modules
import pydoop.hdfs as hdfs

# WMArchive modules
import WMArchive # to know location of the code
from WMArchive.Utils.Regexp import PAT_UID
from WMArchive.Utils.Exceptions import WriteError, ReadError
from WMArchive.Utils.Utils import wmaHash, tstamp, range_dates
from WMArchive.Utils.TaskManager import TaskManager

def make_hdfs_path(hdir, trange):
    """
    Create an HDFS paths to look at from provided main hdfs dir
    and provided time range list.
    """
    return ['%s/%s' % (hdir, d) for d in range_dates(trange)]

class LTSManager(object):
    "Long-Term Storage manager based on HDFS/Spark back-end"
    def __init__(self, uri, wmauri, yarn=''):
        "ctor with LTS uri (hdfs:///path/schema.avsc) and WMArchive uri"
        self.uri = uri
        if  not hdfs.ls(self.uri):
            raise Exception("No avro schema file found in provided uri: %s" % uri)
        self.hdir = self.uri.rsplit('/', 1)[0]
        if  not hdfs.path.isdir(self.hdir):
            raise Exception('HDFS path %s does not exists' % self.hdir)
        schema_doc = hdfs.load(self.uri)
        self.schema = avro.schema.parse(schema_doc)
        self.taskmgr = TaskManager()
        self.wmauri = wmauri # WMArchive URL which will be used by submit
        if  not self.wmauri.endswith('/wmarchive/data'):
            self.wmauri = '%s/wmarchive/data' % self.wmauri
        self.yarn = yarn

    def status(self):
        "Return status of taskmgr"
        return dict(lts=self.taskmgr.status())

    def lmap(self, spec, fields):
        "map input spec/fields into ones suitable for LTS QL"
        return spec, fields

    def write(self, data, safe=None):
        "Write API for LTS, currently we do not provide direct write access to LTS"
        raise NotImplementedError

    def read(self, spec, fields=None):
        "Read API for LTS"
        try:
            if  not spec:
                spec = {}
            if  isinstance(spec, list):
                spec = {'wmaid': {'$in': spec}}
                return self.read_from_storage(spec) # list of wmaids
            elif  PAT_UID.match(str(spec)):
                return self.read_from_storage([spec]) # one wmaid
            else:
                return self.submit(spec, fields)
        except Exception as exp:
            raise ReadError(str(exp))

    def submit(self, spec, fields):
        """
        Submit job to HDFS/Spark platform, returns list of hash ids
        """
        # generate uid for given spec/fields
        rep = json.dumps(dict(spec=spec, fields=fields))
        wmaid = wmaHash(rep)
        # submit spark job
        self.taskmgr.spawn(self.submit_spark, wmaid, spec, fields)
        # self.submit_spark(wmaid, spec, fields)
        # return wmaids of submitted job
        results = [wmaid]
        return results

    def read_from_storage(self, wmaids):
        "Retrieve results from storage for given set of ids"
        # this method provides read access for to STS/HDFS/HBase/Oracle
        # back-end where results will be stored. So far we store results
        # to STS and therefore will read from it.
        return self.sts.read(wmaids)

    def submit_spark(self, wmaid, spec, fields, wait=60):
        """
        Submit function provides interface how to submit job to
        HDFS/Spark/MR. It will use subprocess module to call
        specific function, e.g. bash script (myspark)

        The job parameters includes: HDFS directory pattern, schema file,
        script name, spec file and store uri. The job will be routed to
        yarn cluster. The myspark script will store results back to
        provided store uri, i.e. WMArchive REST interface.
        """
        "Run given command in subprocess"
        hdir = ' '.join(make_hdfs_path(self.hdir, spec.pop('timerange')))
        schema = self.uri
        sfile = 'PySpark/RecordFinder.py'
        if  'aggregate' in spec:
            sfile = 'PySpark/RecordReader.py'
        ppath = '/'.join(WMArchive.__file__.split('/')[:-1])
        script = os.path.join(ppath, sfile)
        data = json.dumps(dict(spec=spec, fields=fields))
        os.environ['PYTHONPATH']=os.environ['PYTHONPATH']+':%s/PySpark' % ppath
        cmd = 'myspark %s --hdir="%s" --schema=%s --script=%s --spec=\'%s\' --store=%s --wmaid=%s' \
                % (self.yarn, hdir, schema, script, data, self.wmauri, wmaid)
        print(tstamp("WMArchive::LTS"), cmd)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, env=os.environ)
        # wait for process if we use taskmgr. The taskmgr has internal queue
        # which controls number of running jobs
        proc.wait()
