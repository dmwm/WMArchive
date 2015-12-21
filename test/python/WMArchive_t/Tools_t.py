#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : Utils_t.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Unit test for WMArchive.Utils.Utils.py module
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

class WMBaseTest(unittest.TestCase):
    def test_genSchema(self):
        "Test genSchema function"
	data = {"int":1, "float":1.2, "list":[1,2,3],
		"dict":{"dname": "foo", "dval":1},
		"listdict":[{"lname":"foo"}], "str":"string"}
        # generate avro schema
        arec = genSchema(data)
        # parse schema into schema object
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
        self.assertEqual(json_data, data)
#
# main
#
if __name__ == '__main__':
    unittest.main()
