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
import re
import sys
import random
import subprocess

# WMArchive modules
from WMArchive.Utils.Utils import cms_filter

# tornado modules
import tornado.ioloop
import tornado.gen

# NATS modules
from nats.io.client import Client as NATS


@tornado.gen.coroutine
def nats(server, subject, msg=None):
    """
    NATS client implemented via tornado (NATS py2 approach), see
    https://github.com/nats-io/nats.py2
    for python3 implementation see
    https://github.com/nats-io/nats.py
    """
    nc = NATS()
    yield nc.connect(server)
    if isinstance(msg, list):
        for item in msg:
            yield nc.publish(subject, item)
    else:
        yield nc.publish(subject, msg)
    # do not close, since it will not send it over
    #yield nc.close()

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
    """
    NATSManager provide python interface to NATS server
    """
    def __init__(self, server=None, topics=None, attrs=None, default_topic='cms-wma', stdout=False):
        self.topics = topics
        self.server = server.split(',')
        self.def_topic = default_topic
        self.stdout = stdout
        self.attrs = attrs

    def repr(self):
        print('NATSManager@{}, server={} topics={} def_topic={} attrs={} stdout={}'.format(self, self.server, self.topics, self.def_topic, self.attrs, self.stdout))

    def publish(self, data):
        "Publish given set of docs to topics"
        cms_msgs = []
        if not self.topics:
            # we will use input docs to get list of sites
            sdict = {}
            for doc in data:
                for rec in cms_filter(doc, self.attrs):
                    subject = rec.get('site', '')
                    if not subject:
                        continue
                    msg = nats_encoder(rec)
                    sdict.setdefault(subject, []).append(msg)
                    cms_msgs.append(msg)
            for topic, msgs in sdict.items():
                self.send(topic, msgs)
            # always send all messages to default topic
            self.send(self.def_topic, cms_msgs)
            return
        for topic in self.topics:
            top_msgs = []
            pat = re.compile(topic)
            for doc in data:
                for rec in cms_filter(doc, self.attrs):
                    msg = nats_encoder(rec)
                    if msg.find(topic) != -1 or pat.match(msg):
                        top_msgs.append(msg) # topic specific messages
                    cms_msgs.append(msg) # cms-wma messages
            self.send(topic, top_msgs)
        # always send all messages to default topic
        self.send(self.def_topic, cms_msgs)

    def send(self, subject, msg):
        "Call NATS function, user can pass either single message or list of messages"
        if self.stdout:
            if isinstance(msg, list):
                for item in msg:
                    print("{}: {}".format(subject, item))
            else:
                print("{}: {}".format(subject, msg))
        else:
            server = random.randint(0, len(self.server))
            tornado.ioloop.IOLoop.current().run_sync(lambda: nats(server, subject, msg))

def nats_pub(subject, msg, server=None, pub=None):
    "NATS publisher via external NATS_PUB tool"
    if not pub:
        pub = os.getenv('NATS_PUB', '')
        if not pub:
            #print('No publication tool found, exit NATS...')
            return
    if not os.path.exists(pub):
        print("Unable to locate {} on local file system".format(pub))
        return
    if not server:
        server = os.getenv('NATS_SERVER', '')
        if not server:
            #print('No NATS server found, exit NATS...')
            return
    cmd = '{} -s {} {} "{}"'.format(pub, server, subject, msg)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, env=os.environ)
    proc.wait()
    return proc.returncode

class NATSManager_pub(object):
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
                    nats_pub(subject, nats_encoder(rec), server=self.server, pub=self.pub)
                    # send to default cms-wma topic too
                    nats_pub('cms-wma', nats_encoder(rec), server=self.server, pub=self.pub)
            return
        for topic in self.topics:
            pat = re.compile(topic)
            for doc in data:
                for rec in cms_filter(doc):
                    msg = nats_encoder(rec)
                    if msg.find(topic) != -1 or pat.match(msg):
                        nats_pub(topic, nats_encoder(rec), server=self.server, pub=self.pub)
                    # send to cms-wma by default
                    nats_pub('cms-wma', nats_encoder(rec), server=self.server, pub=self.pub)

def test():
    "Test function"
    subject = 'cms-wma'
    msg = 'test from python'
    doc = {'site':'1', 'attr':'1'}
    msg = nats_encoder(doc)
    print(msg)
    rec = nats_decoder(msg)
    print(doc, rec)

    data = [{'site':'Site_TEST', 'attr':str(i), 'task':'task-%s'%i} for i in range(10)]
    server = 'nats://test.host.com'
    attrs = ['site', 'attr', 'task']
    mgr = NATSManager(server, attrs=attrs, stdout=True)
    print("Test NATSManager", mgr)
    mgr.publish(data)
    # create new manager with topics
    topics = ['attr']
    mgr = NATSManager(server, topics=topics, attrs=attrs, stdout=True)
    print("Test NATSManager", mgr)
    mgr.publish(data)

if __name__ == '__main__':
    test()
