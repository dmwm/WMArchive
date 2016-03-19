#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : SparkIO.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: WMArchive Spark storage client. This module is responsible
for providing read api to HDFS via Spark interface.
"""

# futures
from __future__ import print_function, division

# system modules
import os
import tempfile
import subprocess

# avro modules
import avro.schema
import avro.io

# hdfs pydoop modules
import pydoop.hdfs as hdfs

# WMArchive modules
import WMArchive # to know location of the code
from WMArchive.Storage.BaseIO import Storage
from WMArchive.Utils.Regexp import PAT_UID
from WMArchive.Utils.Exceptions import WriteError, ReadError
from WMArchive.Utils.Utils import wmaHash, tstamp
from WMArchive.Utils.TaskManager import TaskManager

def make_hdfs_path(hdir, trange):
    """
    Create an HDFS path pattern to look at from provided main hdfs dir
    and user spec which provides timeframe
    """
    # TODO: implement how to provide pattern on HDFS path
    return hdir

class SparkStorage(Storage):
    "Storage based on HDFS/Spark back-end"
    def __init__(self, uri, wmauri):
        "ctor with spark uri: sparkio://hdfspath/schema.avsc"
        Storage.__init__(self, uri)
        schema = self.uri
        if  not hdfs.ls(schema):
            raise Exception("No avro schema file found in provided uri: %s" % uri)
        self.hdir = self.uri.rsplit('/', 1)[0]
        if  not hdfs.path.isdir(self.hdir):
            raise Exception('HDFS path %s does not exists' % self.hdir)
        schema_doc = hdfs.load(schema)
        self.schema = avro.schema.parse(schema_doc)
        self.taskmgr = TaskManager()
        self.wmauri = wmauri # WMArchive URL which will be used by submit

    def sconvert(self, spec, fields):
        "convert input spec/fields into ones suitable for Skark QL"
        return spec, fields

    def write(self, data, safe=None):
        "Write API, return ids of stored documents"
        raise NotImplementedError

    def read(self, spec, fields=None):
        "Read API, it reads data from HDFS/Spark storage for provided spec."
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
        # return wmaids of submitted job
        results = [wmaid]
        return results

    def read_from_storage(self, wmaids):
        "Retrieve results from storage for given set of ids"
        # this method will provide read access for
        # given wmaid(s) to HDFS/HBase/Oracle where results
        # will be stored. The idea is for given wmaids get back results.

    def submit_spark(self, spec, fields, wait=60):
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
        hdir = make_hdfs_path('hdfs://%s' % self.hdir, spec.pop('timerange'))
        schema = 'hdfs://%s' % self.uri
        script = os.path.join('/'.join(WMArchive.__file__.split('/')[:-1]), 'Tools/myspark.py')
        fobj = tempfile.NamedTemporaryFile()
        data = json.dumps(dict(spec=spec, fields=fields))
        fobj.write(data)
        spec_file = fobj.name
        wmaid = wmaHash(data)
        cmd = 'myspark --hdir=%s --schema=%s --script=%s --spec=%s --store=%s --wmaid=%s --yarn-cluster' \
                % (hdir, schema, script, spec_file, self.wmauri, wmaid)
        print(tstamp("WMArchive::SparkStorage"), cmd)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
