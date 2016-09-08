#!/usr/bin/env python
# Author: Nils Fischer <n.fischer@viwid.com>
# Tool to regularly clean aggregated performance data in MongoDB by gradually decreasing precision

import logging
logger = logging.getLogger(__name__)

import datetime
import subprocess
import os

from pymongo import MongoClient

from WMArchive.Tools.fwjr_aggregator import DATE_FORMAT as AGGREGATOR_DATE_FORMAT

MIN_TIME = {
    'hour': datetime.timedelta(hours=48),
    'day': datetime.timedelta(days=7),
    'week': datetime.timedelta(weeks=4),
    'month': datetime.timedelta(weeks=6*4),
}

def get_aggregation_result(cursor_or_dict):
    """
    Fallback for pymongo<3.0
    """
    if type(cursor_or_dict) is dict:
        return cursor_or_dict['result']
    return list(cursor_or_dict)

class OptionParser(object):
    "User based option parser"
    def __init__(self):
        self.parser = argparse.ArgumentParser(prog='clean_performance')
        self.parser.add_argument("--mongo", action="store",\
                dest="muri", default="mongodb://localhost:8230", \
                help="MongoDB URI")

def main():
    # Parse command line arguments
    optmgr  = OptionParser()
    args = optmgr.parser.parse_args()

    mongo_client = MongoClient(args.muri)
    performance_data = mongo_client['aggregated']['performance']

    logging.basicConfig(level=logging.INFO)

    prev_min_date = None
    for precision in [ 'hour', 'day', 'week', 'month' ]:
        precision_ly = precision.replace('y', 'i') + 'ly'

        # Determine earliest date where performance data should have this precision
        min_date = datetime.datetime.now() - MIN_TIME[precision]
        if precision in [ 'hour', 'day', 'week' ]:
            min_date = min_date.replace(hour=0, minute=0, second=0, microsecond=0)
        if precision in [ 'day' ]:
            min_date = min_date - datetime.timedelta(days=min_date.weekday())
        if precision in [ 'week' ]:
            min_date = min_date.replace(day=1)

        # Remove all data earlier that this
        logger.info("Removing all {} data earlier than {}...".format(precision_ly, min_date))
        removal_result = performance_data.remove({
            'scope.timeframe_precision': precision,
            'scope.end_date': { '$lte': min_date},
        })
        logger.info(removal_result)

        if prev_min_date is not None:

            # Find latest date where data of this precision exists
            max_date = (get_aggregation_result(performance_data.aggregate([
                {
                    '$match': { 'scope.timeframe_precision': precision},
                },
                {
                    '$group': {
                        '_id': None,
                        'max_date': { '$max': '$scope.end_date' },
                    }
                },
            ])) or [ {} ])[0].get('max_date')
            start_date = max_date or min_date

            if start_date < prev_min_date:
                logger.info("Latest {} data is from {}, generating from {} to {}...".format(precision_ly, max_date, start_date, prev_min_date))
                subprocess.call([ os.path.join(os.path.dirname(__file__), 'fwjr_aggregator'), '--hdir=/cms/wmarchive/avro/2016', '--precision=' + precision, '--min_date=' + start_date.strftime(AGGREGATOR_DATE_FORMAT), '--max_date=' + prev_min_date.strftime(AGGREGATOR_DATE_FORMAT) ])
                # TODO: dynamically select HDFS path
            else:
                logger.info("Latest {} data is from {}, no need to regenerate data to {}.".format(precision_ly, max_date, prev_min_date))

        prev_min_date = min_date

if __name__ == '__main__':
    main()

