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
import calendar
import datetime

# WMArchive modules
from WMArchive.Utils.Regexp import PAT_YYYYMMDD

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
    if  isinstance(data, dict) or isinstance(data, list):
        data = json.dumps(data)
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

def dateformat(value):
    """Check if provided value in expected WMA date format."""
    msg  = 'Unacceptable date format, value=%s, type=%s,' \
            % (value, type(value))
    msg += " supported format is YYYYMMDD"
    value = str(value)
    if  PAT_YYYYMMDD.match(value): # we accept YYYYMMDD
        if  len(value) == 8: # YYYYMMDD
            ddd = datetime.date(int(value[0:4]), # YYYY
                                int(value[4:6]), # MM
                                int(value[6:8])) # DD
        else:
            raise Exception(msg)
        return calendar.timegm((ddd.timetuple()))
    else:
        raise Exception(msg)

def bulk_avsc(schema):
    """
    Convert given avro schema into bulk one. The avro schema
    covers concrete JSON structure, e.g. {'data':1}.
    The bulk avro schema wraps it into the following sctructure:
    {'bulk':[{'data':1}]}.
    """
    if  isinstance(schema, basestring): # we read it from file
        schema = json.loads(schema)
    wrap = {"namespace": "wma", "type": "record", "name": "records",
            "fields": [{"type": {"items": schema, "type": "array"},
                        "name": "bulk"}]}
    return wrap

def bulk_data(data):
    "Convert given data into bulk data structure"
    if  isinstance(data, list):
        return {'bulk':data}
    return {'bulk':[data]}
