#! /usr/bin/env python

import os, sys
import datetime
BUILD_START_TIME = datetime.datetime.now()

from dateutil.relativedelta import relativedelta

import numpy as N

from atmosci.utils.timeutils import elapsedTime

from atmosci.seasonal.factory import AcisProjectFactory

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()

parser.add_option('-v', action='store_true', dest='verbose', default=False)
parser.add_option('-z', action='store_true', dest='debug', default=False)

options, args = parser.parse_args()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

debug = options.debug
verbose = options.verbose
region_key = args[0]

factory = AcisProjectFactory()
bbox = factory.config.regions[region_key].data
source = factory.getSourceConfig('ndfd')

builder = factory.getStaticFileBuilder('ndfd', region_key)
builder.initFileAttributes()

data = factory.getAcisGridData(source_key, 'mint', date, None, False,
                               meta=('ll','elev'), bbox=bbox, debug=debug)
print builder.filepath

builder.build(True, True, data['lon'], data['lat'], elev_data=data['elev'],
              bbox=bbox)

elapsed_time = elapsedTime(BUILD_START_TIME, True)
print 'completed build of %s static file in' % source.tag, elapsed_time
