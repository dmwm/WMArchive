#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : Methods.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: This module consists of all REST APIs required by WMArchive service
             Every API is designed as a class with appropriate get/post/put/delete
             methods, see RESTEntity class for more details.
"""
# futures
from __future__ import print_function, division

# system modules
import re
import json
import cherrypy
import traceback

# WMCore modules
import WMCore
from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import validate_str
from WMCore.REST.Format import JSONFormat

# WMArchive modules
from WMArchive.Service.Manager import WMArchiveManager

# global regexp
PAT_QUERY = re.compile(r"^[a-zA-Z]+")
PAT_INFO = re.compile(r"^[0-9]$")

class WMAData(RESTEntity):
    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)
        self.config = config
        self.mgr = WMArchiveManager(config)
    
    def validate(self, apiobj, method, api, param, safe):
        """
        Validate request input data.
        Has to be implemented, otherwise the service fails to start.
        If it's not implemented correctly (e.g. just pass), the arguments
        are not passed in the method at all.
        
        """
        if  method == 'GET':
            if 'query' in param.kwargs.keys():
                validate_str('query', param, safe, PAT_QUERY, optional=True)
            if 'info' in param.kwargs.keys():
                validate_str('info', param, safe, PAT_INFO, optional=True)
        elif method == 'POST':
            if  not param.args or not param.kwargs:
                return False # this class does not need any parameters
        return True

    @restcall(formats = [('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def get(self, **kwds):
        "GET request with given query, all the work is done by WMArchiveManager"
        query = kwds.get('query', '')
        if  query:
            docs = self.mgr.read(query)
            return docs
        info = kwds.get('info', '')
        if  info:
            return json.dumps(self.mgr.info())

    @restcall(formats = [('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def post(self):
        "POST request with given data, all the work is done by WMArchiveManager"
        data = {}
        try :
            data = json.load(cherrypy.request.body)
            status = self.mgr.write(data)
        except Exception as exp:
            traceback.print_exc()
            raise cherrypy.HTTPError(str(exp))
