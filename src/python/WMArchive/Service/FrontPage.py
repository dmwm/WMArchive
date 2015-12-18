#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : FrontPage.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: WMArchive front page module which defines its web UI.
             Code design follows SiteDB interface.
"""

# system modules
import os
import re

# WMCore modules
from WMCore.REST.Server import RESTFrontPage

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
        content = os.path.abspath(__file__).rsplit('/', 5)[0]
        xlib = (__file__.find("/xlib/") >= 0 and "x") or ""
        roots = \
        {
          mainroot:
          {
            "root": "%s/%sdata/" % (content, xlib),
            "rx": re.compile(r"^[a-z]+/[-a-z0-9]+\.(?:css|js|png|gif|html)$")
          }

        }
        # location of frontpage in the root, e.g. wmarchive
        uic = config.dictionary_()
        frontpage = "%s/wma.html" % uic.get('templates', '')
        RESTFrontPage.__init__(self, app, config, mount, frontpage, roots)
