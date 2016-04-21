#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : ElasticSearchIO.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: WMArchive ElasticSearch storage client
References :
    - https://github.com/elastic/elasticsearch-py
    - http://elasticsearch-py.readthedocs.org/en/master/index.html
    - http://elasticsearch-py.readthedocs.org/en/master/helpers.html
    - http://elasticsearch-dsl.readthedocs.org/en/latest/
"""

# futures
from __future__ import print_function, division

# system modules
import itertools

# ElasticSearch modules
from elasticsearch import Elasticsearch
from elasticsearch import helpers

# WMArchive modules
from WMArchive.Storage.BaseIO import Storage
from WMArchive.Utils.Regexp import PAT_UID
from WMArchive.Utils.Exceptions import WriteError, ReadError

class ElasticSearchStorage(Storage):
    "Storage based on ElasticSearchDB back-end"
    def __init__(self, uri, dbname='fwjr', collname='db', chunk_size=1000):
        "ctor with mongo uri: elasticsearch://host:port"
        Storage.__init__(self, uri)
        endpoint = uri.replace('elasticsearch', 'http')
        self.client = Elasticsearch([endpoint])
        self.dbname = dbname
        self.collname = collname

    def sconvert(self, spec, fields):
        "convert input spec/fields into ones suitable for ElasticSearch QL"
        return spec, fields

    def data2es(self, data):
        for rec in data:
            yield {'_index':self.dbname, '_type':self.collname, '_source':rec}

    def write(self, data, safe=None):
        "Write API, return ids of stored documents"
        if  not isinstance(data, list):
            data = [data] # ensure that we got list of data
        wmaids = self.getids(data)
        helpers.bulk(self.client, self.data2es(data))
        return wmaids

    def read(self, spec, fields=None):
        "Read API, it reads data from ElasticSearchDB storage for provided spec."
        try:
            gen = self.find(spec, fields)
            docs = [r for r in gen]
            return docs
        except Exception as exp:
            raise ReadError(str(exp))

    def find(self, spec, fields):
        """
        Find records in ElasticSearch storage for provided spec, returns generator
        over ElasticSearch collection
        """
        if  not spec:
            spec = {}
        # TODO: I need to convert match_all to use given spec/fields
        res = self.client.search(index=self.dbname,
                                 doc_type='db',
                                 body={"query": {"match_all": {}}})
        for hit in res['hits']['hits']:
            yield hit["_source"]
