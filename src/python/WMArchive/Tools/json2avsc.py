#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : json2avsc.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Tool to generate Avro schema out of provided JSON file
Avro schema definition can be found here:
    https://avro.apache.org/docs/1.7.7/spec.html#Arrays
    http://docs.oracle.com/cd/E26161_02/html/GettingStartedGuide/avroschemas.html
The conversion logic relies on few constraints:
    - list data type should contain values of identical types, e.g.
      integers, floats, another dictionaries
    - int/floats are stored as long and double
    - primitive types do not include bytes support (it can be easily extended though)
    - complex data types are limited to array/union but no support for Enums and Maps
    - this tool auto generates namespace names, but preserve key names
"""
# futures
from __future__ import print_function, division

# system modules
import json
import argparse
from types import NoneType

COUNTER = 0 # global namespace counter, used to generate proper namespaces

class OptionParser(object):
    "User based option parser"
    def __init__(self):
        self.parser = argparse.ArgumentParser(prog='json2avsc')
        self.parser.add_argument("--fin", action="store", \
            dest="fin", default="", help="Input JSON file")
        self.parser.add_argument("--fout", action="store", \
            dest="fout", default="", help="Output Avro schema file")

def gen_headers():
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
    elif isinstance(val, long):
        return True
    elif isinstance(val, float):
        return True
    elif isinstance(val, bool):
        return True
    elif isinstance(val, NoneType):
        return True
    return False

def stype(val):
    "Return type string representation"
    if isinstance(val, basestring):
        return 'string'
    elif isinstance(val, int):
        return 'long'
    elif isinstance(val, long):
        return 'long'
    elif isinstance(val, bool):
        return 'boolean'
    elif isinstance(val, float):
        return 'double'
    elif isinstance(val, NoneType):
        return 'null'
    return 'null'

def gen_schema(data):
    "Generate Avro schema from provide JSON data"
    fields = []
    for key, val in data.items():
        if  baseTypes(val):
            fields.append({'name':key, 'type': stype(val)})
        elif isinstance(val, list):
            if  not len(val):
                cdict = {'name': key, 'type':{'type':'array', 'items':stype(None)}}
                fields.append(cdict)
            elif baseTypes(val[0]):
                cdict = {'name': key, 'type':{'type':'array', 'items':stype(val[0])}}
                fields.append(cdict)
            elif isinstance(val[0], dict):
                nrec = {'type': 'array', 'items': gen_schema(val[0])}
                fields.append({'name': key, 'type': nrec})
            else:
                raise NotImplementedError
        elif isinstance(val, dict):
            nrec = gen_schema(val)
            fields.append({'name': key, 'type': nrec})
        else:
            raise NotImplementedError
    rec = gen_headers()
    rec['fields'] = fields
    return rec

def gen(fin, fout):
    "Generate Avro schema from provided JSON file"
    with open(fin, 'r') as istream:
        data = json.load(istream)
        with open(fout, 'w') as ostream:
            rec = gen_schema(data)
            ostream.write(json.dumps(rec, indent=4))

def main():
    "Main function"
    optmgr = OptionParser()
    opts = optmgr.parser.parse_args()
    gen(opts.fin, opts.fout)

if __name__ == '__main__':
    main()
