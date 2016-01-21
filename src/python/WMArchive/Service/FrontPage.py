#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : FrontPage.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: WMArchive front page module which defines its web UI.
Code design follows SiteDB interface.
"""

# futures
from __future__ import print_function, division

# system modules
import os
import re

# WMCore modules
from WMCore.REST.Server import RESTFrontPage
from WMArchive.Utils.Utils import tstamp

class FrontPage(RESTFrontPage):
    """WMArchive front page.
    WMArchive provides only one web page, the front page. The page just
    loads the javascript user interface, complete with CSS and all JS
    code embedded into it.

    The JavaScript code performs all the app functionality via the REST
    interface defined by the :class:`~.WMAData` class.
    """

    def __init__(self, app, config, mount):
        """
        :arg app: reference to the application object.
        :arg config: reference to the configuration.
        :arg str mount: URL mount point."""
        mainroot = 'wmarchive' # entry point in access URL
        wpath = os.getenv('WMA_STATIC_ROOT', '')
        if  not wpath:
            content = os.path.abspath(__file__).rsplit('/', 5)[0]
            xlib = (__file__.find("/xlib/") >= 0 and "x") or ""
            wpath = "%s/%sdata/" % (content, xlib)
        if  not wpath.endswith('/'):
            wpath += '/'
        print(tstamp(self.__class__.__name__), "static content: %s" % wpath)
        mdict = {"root": wpath, \
                 "rx": re.compile(r"^[a-z]+/[-a-z0-9]+\.(?:html)$")}
        tdict = {"root": wpath+"templates/", \
                "rx": re.compile(r"^([a-zA-Z]+/)*[-a-z0-9_]+\.(?:html|tmpl)$")}
        jdict = {"root": wpath+"js/", \
                 "rx": re.compile(r"^([a-zA-Z]+/)*[-a-z0-9_]+\.(?:js)$")}
        cdict = {"root": wpath+"css/", \
                 "rx": re.compile(r"^([a-zA-Z]+/)*[-a-z0-9_]+\..*(?:css)$")}
        idict = {"root": wpath+"images/", \
                 "rx": re.compile(r"^([a-zA-Z]+/)*[-a-z0-9_]+\.(?:png|gif|jpg)$")}
        roots = {mainroot: mdict, "templates": tdict, \
                "js": jdict, "css": cdict, "images": idict}
        # location of frontpage in the root, e.g. wmarchive
        frontpage = "%s/templates/wma.html" % mainroot
        RESTFrontPage.__init__(self, app, config, mount, frontpage, roots)
