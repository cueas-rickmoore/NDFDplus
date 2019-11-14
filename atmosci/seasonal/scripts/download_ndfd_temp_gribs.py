#! /Volumes/Transport/venv2/ndfd/bin/python

import os, sys
import datetime
UPDATE_START_TIME = datetime.datetime.now()

import urllib
from dateutil.relativedelta import relativedelta

import numpy as N

from atmosci.utils.timeutils import elapsedTime

from atmosci.seasonal.factory import NDFDProjectFactory

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()

parser.add_option('-n', action='store_true', dest='use_ndfd_cache',
                  default=False)
parser.add_option('-v', action='store_true', dest='verbose', default=False)
parser.add_option('-z', action='store_true', dest='debug', default=False)

options, args = parser.parse_args()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

debug = options.debug
use_ndfd_cache = options.use_ndfd_cache
verbose = options.verbose or debug

latest_time = datetime.datetime.utcnow()
target_year = latest_time.year

factory = NDFDProjectFactory()
if use_ndfd_cache:
    factory.setServerUrl(factory.ndfd_config.cache_server)

target_date, filepaths = factory.downloadLatestForecast(True)

elapsed_time = elapsedTime(UPDATE_START_TIME, True)
fmt = 'completed download for %s in %s' 
print fmt % (target_date.isoformat(), elapsed_time)

