#!/usr/bin/env python
"""
File       : ParquetIO.py
Author     : Luca Menichetti <luca.menichetti AT cern dot ch>
Description: Converts a set of JSONs into Parquet,
             Spark SQLContext is needed (or HiveContext)
"""

import json

class ParquetIO(object):
    def __init__(self, sparkContext, sparkSQLContext):
        self.sqlc = sparkSQLContext
        self.sc = sparkContext

    def file_write(self, fname, data, repartitionNumber=None):
        """
        fname: output folder name, usually a HDFS path
        data: an array of JSONs
        repartitionNumer: [optional] the number of partitions used to write the output file
        """
        if not self.sqlc or not self.sc:
            raise Exception("Both Spark Context and SQLContext have to be available")
        jsonDocsDF = self.sqlc.jsonRDD(self.sc.parallelize([json.dumps(j) for j in data]))
        if repartitionNumber:
            jsonDocsDF.repartition(repartitionNumber).write.parquet(fname)
        else:
            jsonDocsDF.write.parquet(fname)
