#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : Regexp_t.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Unit test for WMArchive.Utils.Regexp.py module
"""
# futures
from __future__ import print_function, division

# system modules
import os
import unittest

# WMArchive modules
from WMArchive.Utils.Regexp import PAT_YYYYMMDD, PAT_YYYY, PAT_MM, PAT_DD

class WMBaseTest(unittest.TestCase):
    def test_pat_yyyymmdd(self):
        "Test PAT_YYYYMMDD pattern"
        date = '20150101'
        res = True if PAT_YYYYMMDD.match(date) else False
        self.assertEqual(res, True)
        date = '201501011'
        res = True if PAT_YYYYMMDD.match(date) else False
        self.assertEqual(res, False)
        date = '30150101'
        res = True if PAT_YYYYMMDD.match(date) else False
        self.assertEqual(res, False)
        date = '20152901'
        res = True if PAT_YYYYMMDD.match(date) else False
        self.assertEqual(res, False)
        date = '20150141'
        res = True if PAT_YYYYMMDD.match(date) else False
        self.assertEqual(res, False)

    def test_pat_yyyy(self):
        "Test PAT_YYYY pattern"
        date = '2015'
        res = True if PAT_YYYY.match(date) else False
        self.assertEqual(res, True)
        date = '2123'
        res = True if PAT_YYYY.match(date) else False
        self.assertEqual(res, False)
        date = '20151'
        res = True if PAT_YYYY.match(date) else False
        self.assertEqual(res, False)
        date = '3015'
        res = True if PAT_YYYY.match(date) else False
        self.assertEqual(res, False)
        date = '1015'
        res = True if PAT_YYYY.match(date) else False
        self.assertEqual(res, False)

    def test_pat_mm(self):
        "Test PAT_YYYY pattern"
        date = '01'
        res = True if PAT_DD.match(date) else False
        self.assertEqual(res, True)
        date = '31'
        res = True if PAT_DD.match(date) else False
        self.assertEqual(res, True)
        date = '32'
        res = True if PAT_DD.match(date) else False
        self.assertEqual(res, False)
        date = '00'
        res = True if PAT_DD.match(date) else False
        self.assertEqual(res, False)

#
# main
#
if __name__ == '__main__':
    unittest.main()
