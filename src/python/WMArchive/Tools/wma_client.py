#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=C0301,C0103,R0914,R0903

"""
WMArchive command line tool
"""
from __future__ import print_function
__author__ = "Valentin Kuznetsov"

# system modules
import os
import sys
import pwd
if  sys.version_info < (2, 6):
    raise Exception("WMArchive requires python 2.6 or greater")

WMArchive_CLIENT = 'wma-client/1.1::python/%s.%s' % sys.version_info[:2]

import re
import time
import json
import urllib
import urllib2
import httplib
import cookielib
from   optparse import OptionParser

# define exit codes according to Linux sysexists.h
EX_OK           = 0  # successful termination
EX__BASE        = 64 # base value for error messages
EX_USAGE        = 64 # command line usage error
EX_DATAERR      = 65 # data format error
EX_NOINPUT      = 66 # cannot open input
EX_NOUSER       = 67 # addressee unknown
EX_NOHOST       = 68 # host name unknown
EX_UNAVAILABLE  = 69 # service unavailable
EX_SOFTWARE     = 70 # internal software error
EX_OSERR        = 71 # system error (e.g., can't fork)
EX_OSFILE       = 72 # critical OS file missing
EX_CANTCREAT    = 73 # can't create (user) output file
EX_IOERR        = 74 # input/output error
EX_TEMPFAIL     = 75 # temp failure; user is invited to retry
EX_PROTOCOL     = 76 # remote error in protocol
EX_NOPERM       = 77 # permission denied
EX_CONFIG       = 78 # configuration error

class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
    """
    Simple HTTPS client authentication class based on provided
    key/ca information
    """
    def __init__(self, key=None, cert=None, level=0):
        if  level > 1:
            urllib2.HTTPSHandler.__init__(self, debuglevel=1)
        else:
            urllib2.HTTPSHandler.__init__(self)
        self.key = key
        self.cert = cert

    def https_open(self, req):
        """Open request method"""
        #Rather than pass in a reference to a connection class, we pass in
        # a reference to a function which, for all intents and purposes,
        # will behave as a constructor
        return self.do_open(self.get_connection, req)

    def get_connection(self, host, timeout=300):
        """Connection method"""
        if  self.key:
            return httplib.HTTPSConnection(host, key_file=self.key,
                                                cert_file=self.cert)
        return httplib.HTTPSConnection(host)

def x509():
    "Helper function to get x509 either from env or tmp file"
    proxy = os.environ.get('X509_USER_PROXY', '')
    if  not proxy:
        proxy = '/tmp/x509up_u%s' % pwd.getpwuid( os.getuid() ).pw_uid
        if  not os.path.isfile(proxy):
            return ''
    return proxy

def check_glidein():
    "Check glideine environment and exit if it is set"
    glidein = os.environ.get('GLIDEIN_CMSSite', '')
    if  glidein:
        msg = "ERROR: das_client is running from GLIDEIN environment, it is prohibited"
        print(msg)
        sys.exit(EX__BASE)

def check_auth(key):
    "Check if user runs das_client with key/cert and warn users to switch"
    if  not key:
        msg  = "WARNING: das_client is running without user credentials/X509 proxy, create proxy via 'voms-proxy-init -voms cms -rfc'"
        print(msg, file=sys.stderr)

def fullpath(path):
    "Expand path to full path"
    if  path and path[0] == '~':
        path = path.replace('~', '')
        path = path[1:] if path[0] == '/' else path
        path = os.path.join(os.environ['HOME'], path)
    return path

class WMArchiveOptionParser: 
    """
    WMArchive cache client option parser
    """
    def __init__(self):
        usage  = "Usage: %prog [options]\n"
        usage += "For more help please visit https://github.com/dmwm/WMArchive/wiki"
        self.parser = OptionParser(usage=usage)
        self.parser.add_option("--verbose", action="store", type="int",
            default=0, dest="verbose", help="verbosity level")
        self.parser.add_option("--spec", action="store", type="string", 
            default="query.json", dest="spec",
            help="specify query spec file, defalt query.json")
        host = 'https://vocms013.cern.ch'
        msg  = "host name of WMArchive server, default is %s" % host
        self.parser.add_option("--host", action="store", type="string", 
                       default=host, dest="host", help=msg)
        msg  = 'specify private key file name, default $X509_USER_PROXY'
        self.parser.add_option("--key", action="store", type="string",
                               default=x509(), dest="ckey", help=msg)
        msg  = 'specify private certificate file name, default $X509_USER_PROXY'
        self.parser.add_option("--cert", action="store", type="string",
                               default=x509(), dest="cert", help=msg)
    def get_opt(self):
        """
        Returns parse list of options
        """
        return self.parser.parse_args()

def get_data(host, spec_file, ckey, cert, verbose=0):
    """Contact WMArchive server and retrieve data for given spec"""
    try:
        query = json.load(open(spec_file))
    except Exception as exp:
        msg = 'Unable to open spec JSON file'
        print(msg)
        print(str(exp))
        sys.exit(EX_DATAERR)
    if  'spec' not in query or 'fields' not in query:
        print('The input query must contain a "spec" and "fields"')
        print('The "spec" represents a dict of conditions, e.g. "spec":{"lfn":"bla.root"} or list of wmaids')
        print('The "fields" provides list of keys to look-up, e.g. "fields":["task"]')
        sys.exit(EX_NOINPUT)
    path = '/wmarchive/data'
    pat  = re.compile('http[s]{0,1}://')
    if  not pat.match(host):
        msg = 'Invalid hostname: %s' % host
        print(msg)
        sys.exit(EX_UNAVAILABLE)
    url = host + path
    data = json.dumps(query)
    client = '%s (%s)' % (WMArchive_CLIENT, os.environ.get('USER', ''))
    headers = {"Accept": "application/json", "User-Agent": client, "Content-type":"application/json"}
    req  = urllib2.Request(url=url, data=data, headers=headers)
    ckey = fullpath(ckey)
    cert = fullpath(cert)
    http_hdlr  = HTTPSClientAuthHandler(ckey, cert, verbose)
    proxy_handler  = urllib2.ProxyHandler({})
    cookie_jar     = cookielib.CookieJar()
    cookie_handler = urllib2.HTTPCookieProcessor(cookie_jar)
    opener = urllib2.build_opener(http_hdlr, proxy_handler, cookie_handler)
    fdesc = opener.open(req)
    response = json.load(fdesc)
    fdesc.close()
    return response

def main():
    """Main function"""
    optmgr  = WMArchiveOptionParser()
    opts, _ = optmgr.get_opt()
    data = get_data(opts.host, opts.spec, opts.ckey, opts.cert, opts.verbose)
    print(json.dumps(data))

#
# main
#
if __name__ == '__main__':
    main()
