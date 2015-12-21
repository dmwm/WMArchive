#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : json2avsc.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Tool to generate Avro schema out of provided JSON file
"""

# system modules
import os
import sys
import json
import pprint
import argparse
from types import NoneType

COUNTER = 0 # global namespace counter, used to generate proper namespaces

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--fin", action="store",
            dest="fin", default="", help="Input JSON file")
        self.parser.add_argument("--fout", action="store",
            dest="fout", default="", help="Output Avro schema file")

def genHeaders():
    "Generate headers for avro types"
    global COUNTER
    namespace = 'ns%s' % COUNTER
    name = "name%s" % COUNTER
    hdict = {"type": "record", "namespace": namespace, "name": name}
    COUNTER += 1
    return hdict

def baseTypes(val):
    "Return if given value is a base type"
    if isinstance(val, basestring):
        return True
    elif isinstance(val, int):
        return True
    elif isinstance(val, float):
        return True
    elif isinstance(val, NoneType):
        return True
    return False

def stype(val):
    "Return type string representation"
    if isinstance(val, basestring):
        return 'string'
    elif isinstance(val, int):
        return 'int'
    elif isinstance(val, float):
        return 'float'
    elif isinstance(val, NoneType):
        return 'null'
    return 'null'

def genSchema(data):
    "Generate Avro schema from provide JSON data"
    fields = []
    for key, val in data.items():
        if  baseTypes(val):
            fields.append({'name':key, 'type': stype(val)})
        elif isinstance(val, list):
            if  not len(val):
                cdict = {'name': key, 'type':{'type':'array','items':stype(None)}}
                fields.append(cdict)
            elif baseTypes(val[0]):
                cdict = {'name': key, 'type':{'type':'array','items':stype(val[0])}}
                fields.append(cdict)
            elif isinstance(val[0], dict):
                nrec = {'type': 'array', 'items': genSchema(val[0])}
                fields.append({'name': key, 'type': nrec})
            else:
                raise NotImplemented
        elif isinstance(val, dict):
            nrec = genSchema(val)
            fields.append({'name': key, 'type': nrec})
        else:
            raise NotImplemented
    rec = genHeaders()
    rec['fields'] = fields
    return rec

def gen(fin, fout):
    "Generate Avro schema from provided JSON file"
    with open(fin, 'r') as istream:
        data = json.load(istream)
        with open(fout, 'w') as ostream:
            rec = genSchema(data)
            ostream.write(json.dumps(rec, indent=4))

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    gen(opts.fin, opts.fout)

if __name__ == '__main__':
    main()
