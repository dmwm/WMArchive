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
import argparse

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--fin", action="store",
            dest="fin", default="", help="Input JSON file")
        self.parser.add_argument("--fout", action="store",
            dest="fout", default="", help="Output Avro schema file")

def genSchema(data):
    "Generate Avro schema from provide JSON data"
    pass

def gen(fin, fout):
    "Generate Avro schema from provided JSON file"
    with open(fin, 'r') as istream:
        data = json.load(istream)
        with open(fout, 'w') as ostream:
            schema = genSchema(data)
            ostream.write(schema)

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    gen(opts.fin, opts.fout)

if __name__ == '__main__':
    main()
