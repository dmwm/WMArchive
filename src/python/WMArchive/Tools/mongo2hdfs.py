#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : mongo2hdfs.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: 
"""

# system modules
import os
import sys
import argparse

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--mongo", action="store",
            dest="muri", default="", help="MongoDB URI")
        self.parser.add_argument("--hdfs", action="store",
            dest="huri", default="", help="HDFS URI")

def migrate(muri, huri):
    "Migrate data from MongoDB (muri) to HDFS (huri)"
    pass

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    migrate(opts.muri, opts.huri)

if __name__ == '__main__':
    main()
