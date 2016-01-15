#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : json2avro.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Script to convert json to avro file
"""
# futures
from __future__ import print_function, division

# system modules
import os
import sys
import json
import argparse

# WMARchive modules
from WMArchive.Storage.MongoIO import MongoStorage
from WMArchive.Storage.AvroIO import AvroStorage

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='mongo2hdfs')
        self.parser.add_argument("--fin", action="store",
            dest="fin", default="", help="Input JSON file")
        self.parser.add_argument("--schema", action="store",
            dest="schema", default="", help="Input Avro schema")
        self.parser.add_argument("--fout", action="store",
            dest="fout", default="", help="Output Avro file")

def migrate(fin, fout, avsc):
    "Migrate data from MongoDB (muri) to HDFS (huri)"
    auri = avsc if avsc.startswith('avroio:') else 'avroio:%s' % avsc
    astg = AvroStorage(auri)

    # read data from MongoDB
    data = json.load(open(fin))

    # store data to Avro
    res = astg.file_write(fout, data)
    wmaid = res.next() # The file_write API is generator and we need to advance
    print("Wrote %s, wmaid=%s" % (fout, wmaid))

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    migrate(opts.fin, opts.fout, opts.schema)

if __name__ == '__main__':
    main()
