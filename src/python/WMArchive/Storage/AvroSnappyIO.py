#!/usr/bin/env python
"""
File       : AvroSnappyIO.py
Author     : Luca Menichetti <luca.menichetti AT cern dot ch>
Description: Converts a set of JSONs into Avro files with Snappy conversion,
Spark SQLContext is needed (or HiveContext)
Usage      : this code can be used as following, write a script as:

    .. code-block:: python

        # test_snappy.py
        import json
        from WMArchive.Storage.AvroSnappyIO import AvroSnappyIO
        from pyspark import SparkContext, SparkConf
        from pyspark.sql import SQLContext

        ctx = SparkContext()
        sqlContext = SQLContext(ctx)

        avro_snappy_IO = AvroSnappyIO(ctx, sqlContext)
        rec = json.load(open('fwjr_prod.json'))
        fwjr_array = [rec, rec]
        avro_snappy_IO.file_write("test-json2avro-snappy",fwjr_array, 0)

To run the code use the following
``spark-submit --packages com.databricks:spark-avro_2.10:1.0.0 test_snappy.py``

Please note, that aforementioned example will work only with new version of spark, 1.5.0 and above.
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
        self.sqlc.setConf("spark.sql.avro.compression.codec", "snappy")
        if repartitionNumber:
            jsonDocsDF.repartition(repartitionNumber).save(fname, "com.databricks.spark.avro", mode=write_mode)
        else:
            jsonDocsDF.save(fname, "com.databricks.spark.avro", mode=write_mode)
