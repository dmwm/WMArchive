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
import sys
import bz2
import gzip
import time
import json
import hashlib
import calendar
import datetime

from gc import get_referents
from types import ModuleType, FunctionType

# https://github.com/nvawda/bz2file
try:
    from bz2file import BZ2File
except:
    pass

# WMArchive modules
from WMArchive.Utils.Regexp import PAT_YYYYMMDD, PAT_YYYY

def write_records(fname, records):
    "Write records to given file name"
    count = 0
    with open(fname, 'w') as ostream:
        ostream.write('[\n')
        for rec in records:
            if  count:
                ostream.write(",\n")
            ostream.write(json.dumps(rec))
            count += 1
        ostream.write("]\n")

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
        data = json.dumps(data, sort_keys=True)
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
    date = str(date)
    return '%s/%s/%s' % (date[0:4], date[4:6], date[6:8])

def range_dates(trange):
    "Provides dates range in HDFS format from given list"
    out = [hdate(str(trange[0]))]
    if  trange[0] == trange[1]:
        return out
    tst = dateformat(trange[0])
    while True:
        tst += 24*60*60
        tdate = time.strftime("%Y%m%d", time.gmtime(tst))
        out.append(hdate(tdate))
        if  str(tdate) == str(trange[1]):
            break
    return out

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

def cms_filter(doc, attrs=None):
    "Function providing CMS filter"
    rec = {}
    if not attrs:
        # we will assume Jen's use case
        rec['task'] = doc.get('task', 'NA')
        rec['campaign'] = doc.get('Campaign', 'NA')
        for row in doc.get('steps', []):
            site = row.get('site', 'NA')
            if not site:
                return
            rec['site'] = site
            for err in row.get('errors', []):
                rec['exitCode'] = err.get('exitCode', -1)
            for out in row['output']:
                rec['dataset'] = out.get('outputDataset', 'NA')
                yield rec
        return
    for attr in attrs:
        if attr in doc:
            rec[attr] = doc[attr]
    yield rec

# Singletons in python
# http://stackoverflow.com/questions/6760685/creating-a-singleton-in-python

# Singleton class
#class Singleton(object):
#    "Implement Singleton behavior"
#    def __new__(cls, *args, **kwargs):
#        "Define single instance and return it back"
#        if  not hasattr(cls, '_instance'):
#            cls._instance = object.__new__(cls, *args, **kwargs)
#        return cls._instance

# Singleton metaclass
class Singleton(type):
    "Implement Singleton behavior as metaclass"
    _instances = {}
    def __call__(cls, *args, **kwargs):
        "Define single instance and return it back"
        if  cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

# code taken from WMCore:
# https://github.com/dmwm/WMCore/blob/master/src/python/Utils/Utilities.py#L159
# so far I'd like to reduce WMCore dependency and remove it completely
def getSize(obj):
    """
    _getSize_
    Function to traverse an object and calculate its total size in bytes
    :param obj: a python object
    :return: an integer representing the total size of the object
    Code extracted from Stack Overflow:
    https://stackoverflow.com/questions/449560/how-do-i-determine-the-size-of-an-object-in-python
    """
    # Custom objects know their class.
    # Function objects seem to know way too much, including modules.
    # Exclude modules as well.
    BLACKLIST = type, ModuleType, FunctionType

    if isinstance(obj, BLACKLIST):
        raise TypeError('getSize() does not take argument of type: '+ str(type(obj)))
    seen_ids = set()
    size = 0
    objects = [obj]
    while objects:
        need_referents = []
        for obj in objects:
            if not isinstance(obj, BLACKLIST) and id(obj) not in seen_ids:
                seen_ids.add(id(obj))
                size += sys.getsizeof(obj)
                need_referents.append(obj)
        objects = get_referents(*need_referents)
    return size
