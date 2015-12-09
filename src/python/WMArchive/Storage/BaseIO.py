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
