#! /Volumes/Transport/venv2/ndfd/bin/python

import os, sys
import datetime
DOWNLOAD_START_TIME = datetime.datetime.now()

from atmosci.utils.timeutils import elapsedTime
from atmosci.seasonal.factory import NDFDProjectFactory

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()

parser.add_option('-d', action='store_true', dest='use_dev_paths',
                  default=False)
parser.add_option('-n', action='store_true', dest='use_ndfd_cache',
                  default=False)
parser.add_option('-v', action='store_true', dest='verbose', default=False)
parser.add_option('-z', action='store_true', dest='debug', default=False)

options, args = parser.parse_args()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

debug = options.debug
use_dev_paths = options.use_dev_paths
use_ndfd_cache = options.use_ndfd_cache
verbose = options.verbose or debug

factory = NDFDProjectFactory()
if use_dev_paths: factory.config.dirpaths.update(factory.config.dev_dirpaths)
if use_ndfd_cache: factory.setServerUrl(factory.ndfd_config.cache_server)

target_date, filepaths = factory.downloadLatestForecast(True)
if verbose:
    print 'NDFD grib files downloaded for %s :', str(target_date)
    for filepath in filepaths:
        print '    ', filepath

elapsed_time = elapsedTime(DOWNLOAD_START_TIME, True)
fmt = 'completed download for %s in %s' 
print fmt % (target_date.isoformat(), elapsed_time)

