#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : diff_avsc2json.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Generic module to diff json record with avro schema.
"""

# system modules
import os
import sys
import json
import pprint
import argparse

class Json2AvscException(Exception):
    """A custom exception to indicate a problem between json record
    and avro schema"""
    def __init__(self, *args, **kwargs):
        super(Json2AvscException, self).__init__(*args, **kwargs)
    def dump(self):
        return str(sys.exc_value)

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
    if  atype == 'bool':
        return isinstance(data, bool)
    if  atype == 'int' or atype == 'long':
        return isinstance(data, int) or isinstance(data, long)
    if  atype == 'float' or atype == 'double':
        return isinstance(data, float)
    if  atype == 'null':
        # null type in avro corresponds either to empty string or None (int/float) data
        res = True if data == '' or data == None else False
        return res

def error(name, avrec, data, refname):
    "Print error message and exit"
    subtype = 'Basic data type'
    if  isinstance(avrec, dict):
        subtype = avrec['type']

    print("\n\n========= ERROR =========\n")
    print('PATH: %s/%s' % (refname, name))
    print('TYPE: %s' % subtype)
    print('AREC: %s' % json.dumps(avrec))
    print('DATA: %s %s' % (json.dumps(data), type(data)))
    msg = 'ERROR path: %s/%s, type: %s, arec: %s, data: %s' \
            % (refname, name, subtype, json.dumps(avrec), json.dumps(data))
    raise Json2AvscException(msg)

def print_details(name, avrec, data, verbose):
    "Print verbose details about given fields"
    if  isinstance(avrec, dict):
        subtype = avrec.get('type', 'NA')
    else:
        subtype = 'Basic data type'
    if  verbose:
        print("\n")
        print("## NAME: %s" % name)
        print("## TYPE: %s" % subtype)
        print("## AREC: %s" % avrec)
        print("## DATA: %s %s" % (data, type(data)))

def check_ctype(name, avrec, data, refname, verbose=0):
    "Check composed data types"
    fields = avrec.get('fields', None)
    print_details(name, avrec, data, verbose)
    subtype = avrec['type']
    refname += '/%s' % name
    if  subtype == 'array':
        # check that data is also array type
        if  not isinstance(data, list):
            error(name, avrec, data, refname)
        items = avrec['items']
        if  isinstance(items, list):
            if  not isinstance(data, list):
                error(name, avrec, data, refname)
            for item in avrec['items']:
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
            error(name, avrec, data, refname)
        elif isinstance(items, dict):
            if  'fields' in items:
                # here items is a new schema, so we use recursion
                if  isinstance(data, list):
                    for rec in data:
                        compare(items, rec, refname, verbose)
                else:
                    compare(items, data, refname, verbose)
    elif subtype == 'record':
        compare(avrec, data, refname, verbose)
    else:
        raise NotImplementedError("Unknown data: %s, type=%s, data=%s" % (name, avrec, data))


def check_btype(name, avrec, data, refname, verbose=0):
    "Check basic data types"
    print_details(name, avrec, data, verbose)
    if  not isinstance(avrec, list):
        arecs = [avrec]
    else:
        arecs = avrec
    for item in arecs:
        if  check_atype(data, item):
            print_ok(data, item, verbose=verbose)
            return True
    error(name, avrec, data, refname)

def print_ok(data, item, dtype=None, verbose=0):
    "Print ok message for given data and schema item"
    if  verbose>1:
        type_data = dtype if dtype else type(data)
        print("CHECK OK data=%s, type(data)=%s, expect=%s" % (data, type_data, item))

def compare(schema, data, refname='', verbose=0):
    "Compare avsc schema with json data"
    if  verbose:
        print("\n### ENTER %s" % refname)
    for item in schema['fields']:
        fname = item['name']
        avrec = item['type']
        fdata = data.get(fname, None)
        stype = None
        if  isinstance(avrec, dict):
            stype = avrec.get('type', None)
        if  stype:
            check_ctype(fname, avrec, fdata, refname, verbose)
        else:
            check_btype(fname, avrec, fdata, refname, verbose)

def validate(data, schema):
    "API to validate data with given schema"
    try:
        compare(schema, data)
    except Json2AvscException as exp:
        return exp.dump()

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    schema = json.load(open(opts.favsc))
    data = json.load(open(opts.fjson))
    refname = '' # empty name, i.e. the root of record
    try:
        compare(schema, data, refname, int(opts.verbose))
    except Json2AvscException as exp:
        sys.exit(1)

if __name__ == '__main__':
    main()
