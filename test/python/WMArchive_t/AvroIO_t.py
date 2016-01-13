#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : AvroIO_t.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Unit test for WMArchive.Storage AvroIO module
"""
# futures
from __future__ import print_function, division

# system modules
import os
import json
import tempfile
import unittest

# avro modules
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

# WMArchive modules
from WMArchive.Tools.json2avsc import genSchema
from WMArchive.Storage.AvroIO import AvroStorage
from WMArchive.Utils.Utils import wmaHash

class FileStorageTest(unittest.TestCase):
    def setUp(self):
        self.tdir = tempfile.mkdtemp()
	data = {"int":1, "float":1.2, "list":[1,2,3],
		"dict":{"dname": "foo", "dval":1},
		"listdict":[{"lname":"foo"}], "str":"string"}
        self.bare_data = dict(data)
        data['wmaid'] = wmaHash(data)
        data['stype'] = 'avroio'
        self.data = data
        schema = genSchema(self.data)
        sname = os.path.join(self.tdir, 'schema.avsc')
        with open(sname, 'w') as ostream:
            ostream.write(json.dumps(schema))
        self.mgr = AvroStorage('avroio:%s' % sname)

    def tearDown(self):
        "Tear down content of temp dir"
        for fname in os.listdir(self.tdir):
            os.remove(os.path.join(self.tdir, fname))
        os.rmdir(self.tdir)

    def test_write(self):
        "Test write functionality"
        wmaids = self.mgr.write(self.data)
        self.assertEqual(len(wmaids), 1)
        data = self.mgr.read(wmaids[0])
        self.assertEqual(data[0], self.bare_data)

    def test_write_bulk(self):
        "Test write functionality"
        bdata = [self.data, self.data]
        bare_data = [self.bare_data, self.bare_data]
        wmaids = self.mgr.write_bulk(bdata)
        self.assertEqual(len(wmaids), 1)
        data = self.mgr.read(wmaids[0])
        self.assertEqual(data, bare_data)
#
# main
#
if __name__ == '__main__':
    unittest.main()
