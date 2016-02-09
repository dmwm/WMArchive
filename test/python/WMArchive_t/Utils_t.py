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
import unittest

# WMArchive modules
from WMArchive.Utils.Utils import wmaHash, dateformat, file_name

class WMBaseTest(unittest.TestCase):
    def test_wmaHash(self):
        "Test wmaHash function"
        rec1 = {'foo':1, 'data':[{'one':1}, {'two':2}]}
        rec2 = {'foo':1, 'data':[{'two':2}, {'one':1}]}
        rec3 = {'data':[{'two':2}, {'one':1}], 'foo':1}
        hsr1 = wmaHash(rec1)
        hsr2 = wmaHash(rec2)
        hsr3 = wmaHash(rec3)
        self.assertEqual(hsr1, hsr2)
        self.assertEqual(hsr1, hsr3)
        self.assertEqual(hsr2, hsr3)

    def test_dateformat(self):
        "Test dateformat function"
        date1 = '20150101'
        res = dateformat(date1)
        self.assertEqual(len(str(res)), 10) # it should be 10 digits
        wrong = 'Some weird format'
        self.assertRaises(Exception, dateformat, wrong)
        wrong = '2012'
        self.assertRaises(Exception, dateformat, wrong)
        date2 = '2d'
        res = dateformat(date1)
        self.assertEqual(len(str(res)), 10) # it should be 10 digits

    def test_file_name(self):
        "Test file_name function"
        uri = 'test'
        wmaid = 123
        for compress in ['', 'bz2', 'gz']:
            fname = file_name(uri, wmaid, compress)
            if  compress:
                tname = '%s/%s.avro.%s' % (uri, wmaid, compress)
            else:
                tname = '%s/%s.avro' % (uri, wmaid)
            self.assertEqual(fname, tname)
        self.assertRaises(Exception, file_name, (uri, wmaid, 'gzip'))
#
# main
#
if __name__ == '__main__':
    unittest.main()
