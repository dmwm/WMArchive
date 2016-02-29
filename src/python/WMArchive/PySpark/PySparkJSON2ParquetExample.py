#!/usr/bin/env python
"""
File       : PySparkJSON2ParquetExample.py
Author     : Luca Menichetti <luca.menichetti AT cern dot ch>
Description: How to convert a set of JSONs into Parquet
"""

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

import json

conf = SparkConf().setAppName("pyspark fwjr JSONs 2 parquet")
sc = SparkContext(conf=conf)
sqlc = SQLContext(sc)


def jsons2Parquet(list_of_docs, output_folder_name):
    """
    This function expects a list of JSON documents and output folder name.
    It should process each document, convert to parquet data format and
    write it out to given output folder
    """
    jsonDocsDF = sqlc.jsonRDD(sc.parallelize([json.dumps(j) for j in list_of_docs]))
    jsonDocsDF.saveAsParquetFile(output_folder_name)
