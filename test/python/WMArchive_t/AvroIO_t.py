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
from tempfile import mkdtemp

# avro modules
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

# WMArchive modules
from WMArchive.Tools.json2avsc import gen_schema
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
        schema = gen_schema(self.data)
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

    def test_file_write(self):
        "Test file_write functionality"
        fname = os.path.join(self.tdir, 'file.avro')
        wmaids = self.mgr.file_write(fname, self.data)
        self.assertEqual(len(wmaids), 1)
        data = self.mgr.file_read(fname)
        self.assertEqual(data[0], self.data)

    def test_file_write_exception(self):
        "Test file_write functionality with exception"
        fname = os.path.join('/etc/file.avro') # we should not have access to /etc
        self.assertRaises(Exception, self.mgr.file_write, (fname, self.data))

#
# main
#
if __name__ == '__main__':
    unittest.main()
