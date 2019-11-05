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
import json

# CMSMonitoring modules
try:
    from CMSMonitoring import StompAMQ
except ImportError:
    StompAMQ = None

def credentials(fname=None):
    "Read credentials from WMA_BROKER environment"
    if  not fname:
        fname = os.environ.get('WMA_BROKER', '')
    if  not os.path.isfile(fname):
        return {}
    with open(fname, 'r') as istream:
        data = json.load(istream)
    return data

def cms_filter(doc, attrs=None):
    "Function providing CMS filter"
    rec = {}
    if not attrs:
        # we will assume Jen's use case
        rec['task'] = doc['task']
        rec['campaign'] = doc['Campaign']
        for row in doc['steps']:
            rec['site'] = row['site']
            for err in row['errors']:
                rec['exitCode'] = err.get('exitCode', -1)
            for out in row['output']:
                rec['dataset'] = out.get('outputDataset', '')
                yield rec
        return
    for attr in attrs:
        if attr in doc:
            rec[attr] = doc[attr]
    yield rec

class MonitManager(object):
    "Monit manager based on CMSMonitoring StompAMQ module"
    def __init__(self, fname=None, attrs=None):
        self.attrs = attrs # our attributes to filter and send to MONIT
        # read our credentials
        creds = credentials(fname)
        # create instance of StompAMQ object with your credentials
        self.amq = None
        if StompAMQ and creds:
            host, port = creds['host_and_ports'].split(':')
            port = int(port)
            self.amq = StompAMQ(creds['username'], creds['password'],
                                creds['producer'], creds['topic'],
                                validation_schema=None,
                                host_and_ports=[(host, port)])

    def write(self, data):
        "Write API for MonitManager"
        if not self.amq:
            return "No StompAMQ module found"
        docs = []
        for doc in data:
            hid = doc.get("hash", 1)
            for rec in cms_filter(doc, self.attrs):
                notification, _, _ = self.amq.make_notification(rec, hid)
                docs.append(notification)
        result = self.amq.send(docs)
        return result
