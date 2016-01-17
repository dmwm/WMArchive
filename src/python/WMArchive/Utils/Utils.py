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
import bz2
import gzip
import time
import json
import hashlib
import calendar
import datetime

# https://github.com/nvawda/bz2file
from bz2file import BZ2File

# WMArchive modules
from WMArchive.Utils.Regexp import PAT_YYYYMMDD

def open_file(fname, mode='r'):
    """
    Helper function to open file, plain, gz, bz2 formats. Returns file descriptor.
    Client is responsible for closing the file. For bz2 we use bz2file helper
    https://github.com/nvawda/bz2file which provides extended set of file
    manipulation features not available in python 2.X up to python 3.4.
    Starting python 3.5 it will no longer required.
    """
    if  fname.endswith('.gz'):
        istream = gzip.open(fname, mode)
    elif fname.endswith('.bz2'):
        # replace with standard bz2 library when we switch to python 3.5
        istream = BZ2File(fname, mode)
    else:
        istream = open(fname, mode)
    return istream

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

def size_format(uinput, base=10):
    """
    Format file size utility, it converts file size into KB, MB, GB, TB, PB units
    """
    try:
        num = float(uinput)
    except Exception as exc:
        print("Fail to read %s, type=%s" % (uinput, type(uinput)))
        print(str(exc))
        return "N/A"
    if  base == 10: # power of 10
        xlist = ['', 'KB', 'MB', 'GB', 'TB', 'PB']
	fbase = 1000.
    elif base == 2: # power of 2
        xlist = ['', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']
	fbase = 1024.
    for xxx in xlist:
        if  num < fbase:
            return "%3.1f%s" % (num, xxx)
        num /= fbase

def file_name(uri, wmaid, compress=None):
    "Construct common file name for given uri and wmaid"
    if  compress and compress not in ['gz', 'bz2']:
        raise Exception("Unsupported compress method %s" % compress)
    fname = '%s/%s.avro' % (uri, wmaid)
    if  compress:
        return '%s.%s' % (fname, compress)
    return fname
