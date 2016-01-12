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
from types import GeneratorType

# WMCore modules
import WMCore
from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import validate_str
from WMCore.REST.Format import JSONFormat

# WMArchive modules
from WMArchive.Service.Manager import WMArchiveManager
from WMArchive.Utils.Regexp import PAT_UID, PAT_QUERY, PAT_INFO

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
            # test if user provided uid
            if len(param.args) == 1 and PAT_UID.match(param.args[0]):
                safe.args.append(param.args[0])
                param.args.remove(param.args[0])
                return True
        elif method == 'POST':
            if  not param.args or not param.kwargs:
                return False # this class does not need any parameters
        return True

    @restcall(formats = [('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def get(self, *args, **kwds):
        """
        Implement GET request with given uid or set of parameters
        All work is done by WMArchiveManager
        """
        info = kwds.get('info', '')
        if  info:
            return json.dumps(self.mgr.info())
        if  args and len(args)==1: # requested uid
            return json.dumps(self.mgr.read(args[0]))
        return json.dumps({'request': kwds, 'results': 'Not available'})

    @restcall(formats = [('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def post(self):
        """
        Implement POST request API, all work is done by WMArchiveManager.
        The request should either provide query to fetch results from back-end
        or data to store to the back-end.

        The input HTTP request should be either
        {"data":some_data} for posting the data into WMArchive or
        {"query":some_query} for querying the data in WMArchive.
        The some_data should be proper JSON document(s).
        The some_query should be either MongoDB or Hive or other supported
        queries.
        """
        result = {'status':'Not supported, expect "data", "query" attributes in your request', 'data':[]}
        try :
            request = json.load(cherrypy.request.body)
            if  'query' in request.keys():
                result = self.mgr.read(request['query'])
            elif 'data' in request.keys():
                result = self.mgr.write(request['data'])
            if  isinstance(result, GeneratorType):
                result = [r for r in result]
#            print("results", result, type(result), isinstance(result, GeneratorType))
            return json.dumps(result)
        except Exception as exp:
            traceback.print_exc()
            raise cherrypy.HTTPError(str(exp))

    @restcall(formats = [('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def put(self):
        """
        Implement PUT request API, all work is done by WMArchiveManager.
        The request should either provide query to fetch results from back-end
        or data to store to the back-end.

        The input HTTP request should be in a form
        {"ids":[list_of_ids], "spec": update_spec}
        """
        result = {'status':'Not supported, expect "data", "query" attributes in your request', 'data':[]}
        try :
            request = json.load(cherrypy.request.body)
            result = self.mgr.update(request['ids'], request['spec'])
            if  isinstance(result, GeneratorType):
                result = [r for r in result]
            return json.dumps(result)
        except Exception as exp:
            traceback.print_exc()
            raise cherrypy.HTTPError(str(exp))
