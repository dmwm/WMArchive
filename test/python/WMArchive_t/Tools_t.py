#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : Tools_t.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Unit test for WMArchive.Tools module
"""
# futures
from __future__ import print_function, division

# system modules
import os
import io
import json
import unittest

# avro modules
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

# WMArchive modules
from WMArchive.Tools.json2avsc import genSchema
from WMArchive.Utils.Utils import bulk_data, bulk_avsc

def json2avro2json(data, bulk=False):
    """
    Function which reads json file, generates avro screma, convert json to
    avro and convert it back using the schema. All operations are done in
    memory (via io.BytesIO)
    """
    # parse schema into schema object
    if  bulk:
        arec = genSchema(data['bulk'][0])
        schema = avro.schema.parse(json.dumps(bulk_avsc(arec)))
    else:
        arec = genSchema(data)
        schema = avro.schema.parse(json.dumps(arec))

    # setup avro writer with given schema
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(data, encoder)

    # our data now is in a bytes_writer, let's get it
    avro_data = bytes_writer.getvalue()

    # read back data into JSON record
    bytes_reader = io.BytesIO(avro_data)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    json_data = reader.read(decoder)
    return json_data

class WMBaseTest(unittest.TestCase):
    def test_genSchema(self):
        "Test genSchema function"
	data = {"int":1, "float":1.2, "list":[1,2,3],
		"dict":{"dname": "foo", "dval":1},
		"listdict":[{"lname":"foo"}], "str":"string"}
        json_data = json2avro2json(data)
        self.assertEqual(json_data, data)

    def test_genSchema_data(self):
        "Test genSchema function with static data"
        tdir = os.path.join('/'.join(__file__.split('/')[:-3]), 'data')
        for fname in os.listdir(tdir):
            if  not fname.endswith('.json'):
                continue
            print("\nRead: %s" % fname)
            with open(os.path.join(tdir, fname)) as istream:
                data = json.load(istream)
                json_data = json2avro2json(data)
                self.assertEqual(json_data, data)

    def test_genSchema_bulk(self):
        "Test genSchema function with bulk data"
        data = {"int":1, "float":1.2, "list":[1,2,3],
                "dict":{"dname": "foo", "dval":1},
                "listdict":[{"lname":"foo"}], "str":"string"}
        bdata = bulk_data(data)
        json_data = json2avro2json(bdata, bulk=True)
        self.assertEqual(json_data, bdata)
#
# main
#
if __name__ == '__main__':
    unittest.main()
