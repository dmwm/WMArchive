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
                if  key in record:
                    del record[key]
            self.assertEqual(record, self.bare_data)
            data = self.mgr.read(wmaids[0], ["dict"])
            self.assertEqual(1, len(data))
            self.assertEqual(data[0]["dict"], self.bare_data["dict"])

#
# main
#
if __name__ == '__main__':
    unittest.main()
