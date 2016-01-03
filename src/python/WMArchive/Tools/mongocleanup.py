#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : mongocleanup.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Mongo clean up script
"""
# futures
from __future__ import print_function, division

# system modules
import os
import sys
import argparse

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='mongocleanup')
        self.parser.add_argument("--mongo", action="store",
            dest="muri", default="", help="MongoDB URI")
        self.parser.add_argument("--tstamp", action="store",
            dest="tstamp", default="", help="Lifetime timestamp")

def cleanup(muri, tstamp):
    "Cleanup data in MongoDB (muri)"
    pass

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    cleanup(opts.muri, opts.tstamp)

if __name__ == '__main__':
    main()
