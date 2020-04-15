#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : Monit.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: WMArchive Monit manager. This module is responsible
for providing write APIs to CERN MONIT infrastructure
"""

# futures
from __future__ import print_function, division

# system modules
import os
import sys
import json
import time
import logging

# CMSMonitoring modules
try:
    from CMSMonitoring.StompAMQ import StompAMQ
except ImportError:
    StompAMQ = None

# WMArchive modules
from WMArchive.Utils.Utils import getSize

def credentials(fname=None):
    "Read credentials from WMA_BROKER environment"
    if  not fname:
        fname = os.environ.get('WMA_BROKER', '')
    if  not os.path.isfile(fname):
        return {}
    with open(fname, 'r') as istream:
        data = json.load(istream)
    return data

class MonitManager(object):
    "Monit manager based on CMSMonitoring StompAMQ module"
    def __init__(self, fname=None, attrs=None, thr=0):
        self.attrs = attrs # our attributes to filter and send to MONIT
        # read our credentials
        self.creds = credentials(fname)
        self.thr = thr if thr else 10<<20 # default 10MB

    def getStompAMQ(self):
        "return StompAMQ instance"
        creds = self.creds
        if StompAMQ and creds:
            host, port = creds['host_and_ports'].split(':')
            port = int(port)
            amq = StompAMQ(creds['username'], creds['password'],
                           creds['producer'], creds['topic'],
                           validation_schema=None,
                           host_and_ports=[(host, port)])
            return amq

    def write(self, data):
        "Write API for MonitManager"
        amq = self.getStompAMQ()
        if not amq:
            return "No StompAMQ module found"
        try:
            docs = []
            for doc in data:
                if '_id' in doc:
                    del doc['_id'] # delete MongoDB ObjectID
                size = getSize(doc)
                if size > self.thr:
                    print("WARNING: doc is too large to be send to MONIT, size: %s" % size)
                    print(json.dumps(doc))
                    continue
                hid = doc.get("hash", 1)
                producer = "wmarchive"
                tstamp = int(time.time())*1000
                notification, _, _ = amq.make_notification(doc, hid, producer=producer, ts=tstamp, dataSubfield="")
                docs.append(notification)
            result = amq.send(docs)
            return result
        except Exception as exc:
            print("Fail to send data to AMQ", str(exc))
