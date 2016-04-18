#!/usr/bin/env python
"""
File       : AvroSnappyIO.py
Author     : Luca Menichetti <luca.menichetti AT cern dot ch>
Description: Converts a set of JSONs into Avro files with Snappy conversion,
             Spark SQLContext is needed (or HiveContext)
"""

import json

class AvroSnappyIO(object):
    def __init__(self, sparkContext, sparkSQLContext):
        self.sqlc = sparkSQLContext
        self.sc = sparkContext

    def file_write(self, fname, data, repartitionNumber=None, write_mode="append"):
        """
        fname: output folder name, usually a HDFS path
        data: an array of JSONs
        repartitionNumer: [optional] the number of partitions used to write the output file
        """
        if not self.sqlc or not self.sc:
            raise Exception("Both Spark Context and SQLContext must be available")
        jsonDocsDF = self.sqlc.jsonRDD(self.sc.parallelize([json.dumps(j) for j in data]))
        sqlContext.setConf("spark.sql.avro.compression.codec", "snappy")
        if repartitionNumber:
            jsonDocsDF.repartition(repartitionNumber).save(fname, "com.databricks.spark.avro", mode=write_mode)
        else:
            jsonDocsDF.save(fname, "com.databricks.spark.avro", mode=write_mode)