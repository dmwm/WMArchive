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
from WMArchive.Utils.Utils import wmaHash

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
#
# main
#
if __name__ == '__main__':
    unittest.main()
