#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : AvroIO.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: WMArchive Avro storage module based on apache Avro python module
https://avro.apache.org/docs/1.7.6/gettingstartedpython.html
"""

# futures
from __future__ import print_function, division

# system modules
import io
import os
import json
import traceback

# avro modules
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

# WMArchive modules
from WMArchive.Storage.BaseIO import Storage
from WMArchive.Utils.Regexp import PAT_UID
from WMArchive.Utils.Utils import wmaHash, open_file, file_name
from WMArchive.Utils.Exceptions import WriteError, ReadError
from WMArchive.Tools.diff_avsc2json import RecordValidator

class AvroStorage(Storage):
    "Storage based on Avro file based back-end"
    def __init__(self, uri):
        "ctor with avro uri: avroio:/path/schema.avsc"
        Storage.__init__(self, uri)
        schema = self.uri
        if  not os.path.exists(schema):
            raise Exception("No avro schema file found in provided uri: %s" % uri)
        self.hdir = self.uri.rsplit('/', 1)[0]
        if  not os.path.exists(self.hdir):
            os.makedirs(self.hdir)
        schema_doc = open(schema).read()
        self.schema = avro.schema.parse(schema_doc)
        self.schema_json = json.loads(schema_doc)

    def file_write(self, fname, data):
        "Write documents in append mode to given file name"
        # perform input data validation
        good_data = []
        # write bad data records into output file
        bdir = '%s/bad' % os.path.dirname(fname)
        if  not os.path.exists(bdir):
            os.makedirs(bdir)
        bfname = '%s/%s_bad.txt' % (bdir, os.path.basename(fname))
        count = ecount = edocs = 0
        with open(bfname, 'a') as bstream:
            for rec in data:
                validator = RecordValidator()
                validator.run(self.schema_json, rec)
                if  validator.errors:
                    bstream.write(json.dumps(rec)+'\n')
                    for err in validator.errors:
                        msg = 'SCHEMA ERROR '
                        for key, val in err.items():
                            msg += '%s: %s ' % (key.upper(), json.dumps(val))
                        bstream.write(msg+'\n')
                    bstream.write('-------------\n')
                    ecount += len(validator.errors)
                    edocs += 1
                else:
                    good_data.append(rec)
                count += 1
        if  ecount:
            print("WARNING: received %s docs, found %s bad docs, %s errors, see %s"\
                    % (count, edocs, ecount, bdir))
        # use only good portion of the data
        data = good_data
        try:
            schema = self.schema
            wmaids = []
            if  not hasattr(data, '__iter__') or isinstance(data, dict):
                data = [data]

            if  os.path.exists(fname):
                schema = None # we'll append to existing file
            mode = 'a+' if fname.endswith('.avro') else 'a'
            if  mode == 'a':
                print("We're unable yet to implement read-write mode with compressed avro files")
                raise NotImplementedError
            rec = None # keep doc in case of failure
            with DataFileWriter(open_file(fname, mode), DatumWriter(), schema) as writer:
                for rec in data:
                    writer.append(rec)
                    writer.flush()
                    wmaid = rec.get('wmaid', wmaHash(rec))
                    wmaids.append(wmaid)
            return wmaids
        except Exception as exc:
            err = traceback.format_exc(limit=1).splitlines()[-1]
            line = ' '.join(str(exc).replace('\n', '').split())
            msg = 'Failure in %s storage, error=%s, exception=%s' \
                    % (self.stype, err, line)
            msg += ' Failed document: '
            msg += json.dumps(rec)
            raise WriteError(msg)

    def file_read(self, fname):
        "Read documents from given file name"
        try:
            schema = self.schema
            out = []
            with DataFileReader(open_file(fname), DatumReader()) as reader:
                for rec in reader:
                    out.append(rec)
            return out
        except Exception as exc:
            err = traceback.format_exc(limit=1).splitlines()[-1]
            msg = 'Failure in %s storage, error=%s' % (self.stype, err)
            raise ReadError(msg)

    def _write(self, data):
        "Internal write API"
        wmaid = self.wmaid(data)
        schema = self.schema
        fname = file_name(self.hdir, wmaid)
        with open_file(fname, 'w') as ostream:
            with DataFileWriter(ostream, DatumWriter(), schema) as writer:
                writer.append(data)

    def _read(self, spec, fields=None):
        "Internal read API"
        if  PAT_UID.match(str(spec)): # requested to read concrete file
            out = []
            fname = file_name(self.hdir, spec)
            with open_file(fname) as istream:
                reader = DataFileReader(istream, DatumReader())
                for data in reader:
                    if  isinstance(data, list):
                        for rec in data:
                            self.check(rec)
                        return data
                    self.check(data)
                    out.append(data)
            return out
        return self.empty_data
