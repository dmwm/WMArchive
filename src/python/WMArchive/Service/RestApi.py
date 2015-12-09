#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : RestApiHub.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: REST API module for WMARchive
"""

# futures
from __future__ import print_function, division

# cherrypy modules
import cherrypy

# WMCore modules
from WMCore.Configuration import Configuration
from WMCore.REST.Server import RESTApi
from WMCore.REST.Format import RawFormat
from WMArchive.Service.Data import WMAData

class RestInterface(RESTApi):
    """
    RestInterface defines REST APIs for WMArchive.
    They are mounted to entry point defined in _add method.
    """
    def __init__(self, app, config, mount):
        """
        :arg app: reference to application object; passed to all entities.
        :arg config: reference to configuration; passed to all entities.
        :arg str mount: API URL mount point; passed to all entities."""
        
        RESTApi.__init__(self, app, config, mount)
        
        cherrypy.log("WMArchive entire configuration:\n%s" % Configuration.getInstance())    
        cherrypy.log("WMArchive REST configuration subset:\n%s" % config)
        
        self._add({"data": WMAData(app, self, config, mount)})
