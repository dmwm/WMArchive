#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
Mongo       : MongoIO_t.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Unit test for WMArchive.Storage MongoIO module
"""
# futures
from __future__ import print_function, division

# system modules
import os
import tempfile
import unittest

# WMArchive modules
from WMArchive.Storage.MongoIO import MongoStorage
from WMArchive.Utils.Utils import wmaHash

class MongoStorageTest(unittest.TestCase):
    def setUp(self):
        uri = os.environ.get('WMA_MONGODB', 'mongodb://localhost:8230')
        self.dbname = 'test_fwjr'
        try:
            self.mgr = MongoStorage(uri, dbname=self.dbname)
            self.mgr.remove()
        except:
            self.mgr = None
            print('WARNING: cannot connect to %s' % uri)
	data = {"int":1, "float":1.2, "list":[1,2,3],
		"dict":{"dname": "foo", "dval":1},
		"listdict":[{"lname":"foo"}], "str":"string"}
        self.bare_data = dict(data)
        data['wmaid'] = wmaHash(data)
        data['stype'] = 'mongodb'
        self.data = data

    def tearDown(self):
        "Tear down content of temp dir"
        self.mgr.remove()
        self.mgr.dropdb(self.dbname)

    def test_write(self):
        "Test write functionality"
        if  self.mgr:
            wmaids = self.mgr.write(self.data)
            self.assertEqual(len(wmaids), 1)
            data = self.mgr.read(wmaids[0])
            record = data[0]
            for key in ['wmaid', 'stype']:
                del record[key]
            self.assertEqual(record, self.bare_data)

    def test_write_bulk(self):
        "Test write functionality"
        data1 = dict(self.bare_data)
        data1['idx'] = 1
        data2 = dict(self.bare_data)
        data2['idx'] = 2
        bare_data = [data1, data2]

        bdata1 = dict(data1)
        bdata1['wmaid'] = wmaHash(data1)
        bdata1['stype'] = 'mongodb'
        bdata2 = dict(data2)
        bdata2['wmaid'] = wmaHash(data2)
        bdata2['stype'] = 'mongodb'
        bdata = [bdata1, bdata2]

        wmaids = self.mgr.write_bulk(bdata)
        self.assertEqual(len(wmaids), 2) # in Mongo bulk still writes separate docs
        data = self.mgr.read()
        out = []
        for rec in data:
            for key in ['wmaid', 'stype']:
                del rec[key]
            out.append(rec)
        self.assertEqual(out, bare_data)
#
# main
#
if __name__ == '__main__':
    unittest.main()
