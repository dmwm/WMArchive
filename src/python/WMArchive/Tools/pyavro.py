#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : pyavro.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: A tool to deal with json to avro conversion and reading avro files
References:
https://avro.apache.org/docs/1.7.6/gettingstartedpython.html
http://hadooptutorial.info/avro-serialization-deserialization-python-api/
http://gisgeek.blogspot.com/2012/12/using-apache-avro-with-python.html
"""

import json
import pprint
import argparse

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

class OptionParser(object):
    "User based option parser"
    def __init__(self):
        prog = 'pyavro'
        usage  = "%s [options]\n\n" % prog
        usage += "A tool to deal with json to avro conversion and read avro files\n"
        usage += "to convert json to avro:\n"
        usage += "   %s --fin=doc.json --schema=schema.avsc --fout=doc.avro\n" % prog
        usage += "to read avro file and dump its content to stdout:\n"
        usage += "   %s --fin=doc.avro\n" % prog
        usage += "to read avro file and store json to output file:\n"
        usage += "   %s --fin=doc.avro --fout=doc.json\n" % prog
        self.parser = argparse.ArgumentParser(usage=usage)
        self.parser.add_argument("--fin", action="store", \
            dest="fin", default="", help="Input JSON file")
        self.parser.add_argument("--schema", action="store", \
            dest="schema", default="", help="Input Avro schema")
        self.parser.add_argument("--fout", action="store", \
            dest="fout", default="", help="Output Avro file")


def write(fin, fout, schema):
    "write json to avro"
    schema = avro.schema.parse(open(schema).read())
    data = json.load(open(fin, 'r'))
    writer = DataFileWriter(open(fout, "w"), DatumWriter(), schema)
    if  isinstance(data, list):
        for doc in data:
            writer.append(doc)
    else:
        writer.append(data)
    writer.close()

def read(fin, fout=None):
    "Read given avro file according to its schema and dump on stdout its content"
    reader = DataFileReader(open(fin, "r"), DatumReader())
    fobj = open(fout, 'w') if fout else None
    for rec in reader:
        if  fobj:
            fobj.write(json.dumps(rec))
        else:
            pprint.pprint(rec)
    if  fobj:
        fobj.close()
    reader.close()

def main():
    "Main function"
    optmgr = OptionParser()
    opts = optmgr.parser.parse_args()
    if  opts.fout and opts.fin.endswith('json'):
        write(opts.fin, opts.fout, opts.schema)
    else:
        read(opts.fin, opts.fout)

if __name__ == '__main__':
    main()
