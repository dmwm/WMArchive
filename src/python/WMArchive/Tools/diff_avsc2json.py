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

def print_ok(data, item, dtype=None, verbose=0):
    "Print ok message for given data and schema item"
    if  verbose>1:
        type_data = dtype if dtype else type(data)
        print("CHECK OK data=%s, type(data)=%s, expect=%s" % (data, type_data, item))

def print_error(err):
    "Helper function to print the error dict"
    print('\n ======= ERROR =======\n')
    for key, val in err.items():
        print('%s: %s' % (key.upper(), val))
    print()

def print_errors(errors):
    "Print errors"
    if  not errors:
        return
    msg = "\nERROR: found %s errors" % len(errors)
    print(msg)
    print('='*len(msg))
    for err in errors:
        for key, val in err.items():
            print('%s: %s' % (key.upper(), val))
        print('-----')

class RecordValidator(object):
    def __init__(self, verbose=0):
        self.verbose = int(verbose)
        self.errors = []

    def record_error(self, name, avrec, data, refname):
        "Print error message and exit"
        subtype = 'Basic data type'
        if  isinstance(avrec, dict):
            subtype = avrec['type']
        if  refname.endswith(name):
            path = refname
        else:
            path = '/'.join([refname, name])
        rec = {'path': path, 'type': subtype, 'arec': avrec, 'data': data}
        if  self.verbose:
            print_error(rec)
        self.errors.append(rec)

    def check_ctype(self, name, avrec, data, refname):
        "Check composed data types"
        fields = avrec.get('fields', None)
        print_details(name, avrec, data, self.verbose)
        subtype = avrec['type']
        refname += '/%s' % name
        if  subtype == 'array':
            # check that data is also array type
            if  not isinstance(data, list):
                self.record_error(name, avrec, data, refname)
                return
            items = avrec['items']
            if  isinstance(items, list):
                if  not isinstance(data, list):
                    self.record_error(name, avrec, data, refname)
                for item in avrec['items']:
                    if  not data and item == 'null':
                        print_ok(data, item, verbose=self.verbose)
                        return
                    oklist = []
                    for rec in data:
                        if  check_atype(rec, item):
                            oklist.append(type(rec))
                    if  oklist and len(oklist) == len(data):
                        print_ok(data, item, oklist, self.verbose)
                        return
                self.record_error(name, avrec, data, refname)
            elif isinstance(items, dict):
                if  'fields' in items:
                    # here items is a new schema, so we use recursion
                    if  isinstance(data, list):
                        for rec in data:
                            self.run(items, rec, refname)
                    else:
                        self.run(items, data, refname)
        elif subtype == 'record':
            self.run(avrec, data, refname)
        else:
            raise NotImplementedError("Unknown data: %s, type=%s, data=%s" % (name, avrec, data))

    def check_btype(self, name, avrec, data, refname):
        "Check basic data types"
        print_details(name, avrec, data, self.verbose)
        if  not isinstance(avrec, list):
            arecs = [avrec]
        else:
            arecs = avrec
        for item in arecs:
            if  check_atype(data, item):
                print_ok(data, item, verbose=self.verbose)
                return True
        self.record_error(name, avrec, data, refname)

    def run(self, schema, data, refname=''):
        "Run comparison of json data wrt avsc schema"
        if  self.verbose:
            print("\n### ENTER %s" % refname)
        for item in schema['fields']:
            fname = item['name']
            avrec = item['type']
            stype = None
            if  isinstance(avrec, dict):
                stype = avrec.get('type', None)
            if  isinstance(data, dict):
                fdata = data.get(fname, None)
            else:
                fdata = None
            if  stype:
                self.check_ctype(fname, avrec, fdata, refname)
            else:
                self.check_btype(fname, avrec, fdata, refname)

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    schema = json.load(open(opts.favsc))
    data = json.load(open(opts.fjson))
    validator = RecordValidator(opts.verbose)
    validator.run(schema, data)
    print_errors(validator.errors)

if __name__ == '__main__':
    main()
