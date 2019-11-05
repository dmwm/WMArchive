#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : NATS.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: python implementation of NATS publisher based on external tool
(go get github.com/nats-io/go-nats-examples/tools/nats-pub)
"""

# system modules
import os
import sys
import argparse
import subprocess

# WMArchive modules
from WMArchive.Utils.Utils import cms_filter

def nats(subject, msg, server=None, pub=None):
    "NATS publisher via external NATS_PUB tool"
    if not pub:
        pub = os.getenv('NATS_PUB', '')
        if not pub:
            return
    if not server:
        server = os.getenv('NATS_SERVER', '')
        if not server:
            return
    cmd = '{} -s {} {} "{}"'.format(pub, server, subject, msg)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, env=os.environ)
    proc.wait()
    return proc.returncode

def nats_encoder(doc):
    "CMS NATS message encoder"
    keys = sorted(doc.keys())
    msg = '___'.join(['{}:{}'.format(k, doc[k]) for k in keys])
    return msg

def nats_decoder(msg):
    "CMS NATS message decoder"
    rec = {}
    for pair in msg.split('___'):
        arr = pair.split(':')
        rec.update({arr[0]:arr[1]})
    return rec

class NATSManager(object):
    "NATS manager for WMArchive"
    def __init__(self, server=None, pub=None, topics=None):
        self.topics = topics
        self.server = server
        self.pub = pub

    def publish(self, data):
        "Publish given set of docs to topics"
        if not self.topics:
            # we will use input docs to get list of sites
            for doc in data:
                for rec in cms_filter(doc):
                    subject = rec.get('site', '')
                    if not subject:
                        continue
                    nats(subject, nats_encoder(rec), server=self.server, pub=self.pub)
            return
        for topic in self.topics:
            for doc in data:
                for rec in cms_filter(doc):
                    nats(topic, nats_encoder(rec), server=self.server, pub=self.pub)

def test():
    "Main function"
    subject = 'cms-test'
    msg = 'test from python'
#     res = nats(subject, msg)
#     print("return code", res)
    doc = {'site':'1', 'attr':'1'}
    msg = nats_encoder(doc)
    print(msg)
    rec = nats_decoder(msg)
    print(doc, rec)

if __name__ == '__main__':
    test()
