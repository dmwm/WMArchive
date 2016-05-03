#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : diff_avsc2json.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: 
"""

# system modules
import os
import sys
import json
import pprint
import argparse
import traceback

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--favsc", action="store",
            dest="favsc", default="", help="Input avsc file (Avro schema)")
        self.parser.add_argument("--fjson", action="store",
            dest="fjson", default="", help="Input json file")
        self.parser.add_argument("--verbose", action="store",
            dest="verbose", default=0, help="Verbosity level")

def check_atype(data, atype):
    "Check data type for given avro data type"
    if  atype == 'string':
        return isinstance(data, basestring)
    if  atype == 'int' or atype == 'long':
        return isinstance(data, int)
    if  atype == 'float' or atype == 'double':
        return isinstance(data, float)
    if  atype == 'null':
        res = False if data else True
        return res

def error(name, ftype, data, refname):
    "Print error message and exit"
    subtype = ftype['type']
    print("\n\n========= ERROR =========\n")
    print('KEY : %s' % refname)
    print('NAME: %s' % name)
    print('TYPE: %s' % subtype)
    print('AREC: %s' % ftype)
    print('DATA: %s' % data)
    sys.exit(1)

def print_details(name, ftype, data, verbose):
    "Print verbose details about given fields"
    if  isinstance(ftype, dict):
        subtype = ftype.get('type', 'NA')
    else:
        subtype = 'Basic data type'
    if  verbose:
        print("\n")
        print("## NAME: %s" % name)
        print("## TYPE: %s" % subtype)
        print("## AREC: %s" % ftype)
        print("## DATA: %s" % data)

def check_ctype(name, ftype, data, refname, verbose=0):
    "Check composed data types"
    fields = ftype.get('fields', None)
    print_details(name, ftype, data, verbose)
    subtype = ftype['type']
    refname += '/%s' % name
    if  subtype == 'array':
        # check that data is also array type
        if  not isinstance(data, list):
            error(name, ftype, data, refname)
        items = ftype['items']
        if  isinstance(items, list):
            for item in ftype['items']:
                if  not data and item == 'null':
                    print_ok(data, item, verbose=verbose)
                    return True
                oklist = []
                for rec in data:
                    if  check_atype(rec, item):
                        oklist.append(type(rec))
                if  oklist and len(oklist) == len(data):
                    print_ok(data, item, oklist, verbose)
                    return True
            error(name, ftype, data, refname)
        elif isinstance(items, dict):
            if  'fields' in items:
                # here items is a new schema, so we use recursion
                if  isinstance(data, list):
                    for rec in data:
                        compare(items, rec, refname, verbose)
                else:
                    compare(items, data, refname, verbose)
    elif subtype == 'record':
        print("check record", name)
    else:
        raise NotImplementedError("Unknown data: %s, type=%s, data=%s" % (name, ftype, data))


def check_btype(name, ftype, data, refname, verbose=0):
    "Check basic data types"
    print_details(name, ftype, data, verbose)
    if  not isinstance(ftype, list):
        types = [ftype]
    else:
        types = ftype
    for item in types:
        if  check_atype(data, item):
            print_ok(data, item, verbose=verbose)
            return True
    error(name, ftype, data, refname)

def print_ok(data, item, dtype=None, verbose=0):
    "Print ok message for given data and schema item"
    if  verbose>1:
        type_data = dtype if dtype else type(data)
        print("CHECK OK data=%s, type(data)=%s, expect=%s" % (data, type_data, item))

def compare(schema, data, refname='', verbose=0):
    "Compare avsc schema with json data"
    print("\n### ENTER %s" % refname)
#     print("schema keys", schema.keys())
#     print("data", type(data))
    for item in schema['fields']:
        fname = item['name']
        ftype = item['type']
        fdata = data.get(fname, None)
        stype = None
        if  isinstance(ftype, dict):
            stype = ftype.get('type', None)
        if  stype:
            check_ctype(fname, ftype, fdata, refname, verbose)
        else:
            check_btype(fname, ftype, fdata, refname, verbose)

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    schema = json.load(open(opts.favsc))
    data = json.load(open(opts.fjson))
    refname = '/' # empty name, i.e. the root of record
    compare(schema, data, refname, int(opts.verbose))

if __name__ == '__main__':
    main()
