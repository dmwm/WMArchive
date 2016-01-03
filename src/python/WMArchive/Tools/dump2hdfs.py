#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : dump2hdfs.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Write given Avro schema file into HDFS
"""
# futures
from __future__ import print_function, division

# system modules
import os
import sys
import argparse

# WMArchive modules
from WMArchive.Storage.HdfsIO import HdfsStorage

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='dump2hdfs')
        self.parser.add_argument("--fin", action="store",
            dest="fin", default="", help="Input avro schema file")

def write(fin, huri):
    "Write fiven file into HDFS"
    hstg = HdfsStorage(huri)
    data = open(fin).read()
    path = huri.split(':', 1)[-1]
    hstg.dump(data, path)

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    write(opts.fin, opts.huri)

if __name__ == '__main__':
    main()
