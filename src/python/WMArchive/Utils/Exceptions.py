#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : Exceptions.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: WMArchive exception classes
"""
# futures
from __future__ import print_function, division

# system modules
import time
from exceptions import Exception

# WMArchive modules
from WMArchive.Utils.Utils import tstamp

class WriteError(Exception):
    "WMArchive write error class"
    def __init__(self, message):
        super(WriteError, self).__init__(message)
        self.message = message
    def __str__(self):
        error = tstamp(repr(self.message))
        return error

class ReadError(Exception):
    "WMArchive read error class"
    def __init__(self, message):
        super(ReadError, self).__init__(message)
        self.message = message
    def __str__(self):
        error = tstamp(repr(self.message))
        return error
