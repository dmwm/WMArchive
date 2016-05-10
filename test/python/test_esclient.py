#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : test_esclient.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: 
"""

import json

from WMArchive.Storage.ElasticSearchIO import ElasticSearchStorage
from WMArchive.Schemas.FWJRProduction import fwjr

uri = 'elasticsearch://localhost:9200'
storage = ElasticSearchStorage(uri)
data = []
for idx in range(10):
    row = dict(fwjr)
    row['task'] = 'abc_%s' % idx
    row['wmaid'] = idx
    row['stype'] = 'elasticsearch'
    data.append(row)

wmaids = storage.write(data)
print("Wrote", wmaids)

print("Read documents")
spec = {}
fields = []
docs = storage.read(spec, fields)
for doc in docs:
    print(doc)
