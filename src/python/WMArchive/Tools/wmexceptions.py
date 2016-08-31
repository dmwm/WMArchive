#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : wmexceptions.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Extract WMCore exceptions and present them as unique dict
"""

# system modules
import os
import sys
import json
import argparse

from WMCore.WMExceptions import WMEXCEPTION, WM_JOB_ERROR_CODES, STAGEOUT_ERRORS

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--fout", action="store",
            dest="fout", default="", help="Output file")

def exceptions():
    "Get exceptions from WMCore and return error dict"
    edict = {}
    for key, val in WMEXCEPTION.items():
        edict[key] = val
    for key, val in WM_JOB_ERROR_CODES.items():
        edict[key] = val
    for key, val in STAGEOUT_ERRORS.items():
        for item in val:
            emsg = item['error-msg']
            edict[key] = emsg
    return edict


def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    with open(opts.fout, 'w') as ostream:
        edict = exceptions()
        ostream.write(json.dumps(edict))

if __name__ == '__main__':
    main()
