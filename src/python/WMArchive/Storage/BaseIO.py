#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : BaseIO.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Base class to define storage APIs
"""

# futures
from __future__ import print_function, division

from WMArchive.Utils.Utils import wmaHash

class Storage(object):
    "Base class which defines storage APIs"
    def __init__(self, uri=None):
        self.uri = uri
    def write(self, data):
        "Write data to local storage"
        pass
    def read(self, query=None):
        "Read data from local storage for given query"
        pass
    def check(self, data):
        "Cross-check the data based on its uid"
        try:
            uid = data.pop('uid')
        except:
            uid = ''
        hid = wmaHash(data)
        if  hid != uid:
            raise Exception("Invalid data hash, hid=%s, uid=%s, data=%s" \
                    % (hid, uid, data))
