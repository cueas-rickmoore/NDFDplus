#! /Volumes/Transport/venv2/ndfd/bin/python

import os, sys
import datetime
BUILD_START_TIME = datetime.datetime.now()
import warnings

from atmosci.utils.timeutils import elapsedTime
from atmosci.seasonal.factory import SeasonalProjectFactory

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()

parser.add_option('-r', action='store', dest='region', default=None)
parser.add_option('-s', action='store', dest='source', default=None)
parser.add_option('-v', action='store_true', dest='verbose', default=False)
parser.add_option('-z', action='store_true', dest='debug', default=False)

options, args = parser.parse_args()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

debug = options.debug
verbose = options.verbose

factory = SeasonalProjectFactory()
project = factory.getProjectConfig()

region = options.region
if region is None: region = project.region
if len(region) == 2: region = region.upper()

source = options.source
if source is None: source = project.source

if len(args) == 0: target_year = BUILD_START_TIME.year
elif len(args) > 0: target_year = int(args[0])


# filter annoying numpy warnings
warnings.filterwarnings('ignore',"All-NaN axis encountered")
warnings.filterwarnings('ignore',"All-NaN slice encountered")
warnings.filterwarnings('ignore',"invalid value encountered in greater")
warnings.filterwarnings('ignore',"invalid value encountered in less")
warnings.filterwarnings('ignore',"Mean of empty slice")

factory.buildSourceFile(source, target_year, region, 'temps', debug=debug,
                        verbose=verbose)

warnings.resetwarnings() # turn annoying numpy warnings back on

elapsed_time = elapsedTime(BUILD_START_TIME, True)
print 'completed build for %d in %s' % (target_year, elapsed_time)

