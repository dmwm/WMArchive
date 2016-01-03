#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : Utils.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: WMArchive utilities
"""

# futures
from __future__ import print_function, division

# system modules
import time
import json
import hashlib

def tstamp(msg='WMA'):
    """
    Return timestamp in pre-defined format. For simplicity we match
    cherrypy date format.
    """
    tst = time.localtime()
    tstamp = time.strftime('[%d/%b/%Y:%H:%M:%S]', tst)
    return '%s %s %s' % (tstamp, time.mktime(tst), msg.strip())

def wmaHash(data):
    "Return md5 hash of given data"
    if  not isinstance(data, basestring):
        data = str(data)
    data = ''.join(sorted(data)) # to insure that all keys are in order
    rec = json.JSONEncoder(sort_keys=True).encode(data)
    keyhash = hashlib.md5()
    keyhash.update(rec)
    return keyhash.hexdigest()

def today():
    "Return today representation YYYY, MM, DD"
    tst = time.localtime()
    year = time.strftime('%Y', tst)
    month = time.strftime('%02m', tst)
    date = time.strftime('%02d', tst)
    return year, month, date
