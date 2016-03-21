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
from WMArchive.Utils.Regexp import PAT_YYYYMMDD, PAT_YYYY

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
    return '%s %s %s ' % (tstamp, time.mktime(tst), msg.strip())

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

def hdate(date):
    "Transform given YYYYMMDD date into HDFS dir structure YYYY/MM/DD"
    return '%s/%s/%s' % (date[0:4], date[4:6], date[6:8])

def range_dates(trange):
    "Provides dates range in HDFS format from given list"
    out = [hdate(trange[0])]
    tst = trange[0]
    while True:
        tst += 24*60*60
        tdate = time.strftime("%Y%m%d", time.gmtime(tst))
        out.append(hdate)
        if  tdate == trange[1]:
            break

def check_tstamp(value):
    "Check that given value conform YYYYMMDD time format"
    value = str(value).lower()
    if  PAT_YYYYMMDD.match(value):
        return True
    elif value.endswith('d'):
        return True
    return False

def dateformat(value):
    """Return seconds since epoch for provided YYYYMMDD or number with suffix 'd' for days"""
    msg  = 'Unacceptable date format, value=%s, type=%s,' \
            % (value, type(value))
    msg += " supported format is YYYYMMDD or number with suffix 'd' for days"
    value = str(value).lower()
    if  PAT_YYYYMMDD.match(value): # we accept YYYYMMDD
        if  len(value) == 8: # YYYYMMDD
            year = value[0:4]
            if  not PAT_YYYY.match(year):
                raise Exception(msg + ', fail to parse the year part, %s' % year)
            month = value[4:6]
            date = value[6:8]
            ddd = datetime.date(int(year), int(month), int(date))
        else:
            raise Exception(msg)
        return calendar.timegm((ddd.timetuple()))
    elif value.endswith('d'):
        try:
            days = int(value[:-1])
        except ValueError:
            raise Exception(msg)
        return time.time()-days*24*60*60
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

def htime(seconds):
    "Convert given seconds into human readable form of N hour(s), N minute(s), N second(s)"
    minutes, secs = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    def htimeformat(msg, key, val):
        "Helper function to proper format given message/key/val"
        if  val:
            if  msg:
                msg += ', '
            msg += '%d %s' % (val, key)
            if  val > 1:
                msg += 's'
        return msg

    out = ''
    out = htimeformat(out, 'day', days)
    out = htimeformat(out, 'hour', hours)
    out = htimeformat(out, 'minute', minutes)
    out = htimeformat(out, 'second', secs)
    return out

def elapsed_time(time0):
    "Return elapsed time from given time stamp"
    return htime(time.time()-time0)
