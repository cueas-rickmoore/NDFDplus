#! /Volumes/Transport/venv2/ndfd/bin/python

import os, sys
import datetime

from atmosci.utils.timeutils import diffInHours
from atmosci.seasonal.factory import SeasonalStaticFileFactory
from atmosci.ndfd.factory import NdfdGridFileFactory

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from atmosci.ndfd.config import CONFIG

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()

parser.add_option('-r', action='store', dest='grid_region',
                        default=CONFIG.sources.ndfd.grid.region)
parser.add_option('-s', action='store', dest='grid_source',
                        default=CONFIG.sources.ndfd.grid.source)

parser.add_option('-d', action='store_true', dest='dev_mode', default=False)
parser.add_option('-u', action='store_false', dest='utc_file', default=True)
parser.add_option('-v', action='store_true', dest='verbose', default=False)
parser.add_option('-z', action='store_true', dest='debug', default=False)

parser.add_option('--next', action='store_true', dest='next_month',
                  default=False)

parser.add_option('--fileltz', action='store', dest='file_timezone',
                        default=CONFIG.sources.ndfd.grid.file_timezone)
parser.add_option('--grib_region', action='store', dest='grib_region',
                        default=CONFIG.sources.ndfd.grib.region)
parser.add_option('--gribtz', action='store', dest='grib_timezone',
                        default=CONFIG.sources.ndfd.grib.timezone)
parser.add_option('--localtz', action='store', dest='local_timezone',
                        default=CONFIG.project.local_timezone)

options, args = parser.parse_args()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

debug = options.debug
dev_mode = options.dev_mode
file_timezone = options.file_timezone
grib_region = options.grib_region
grib_timezone = options.grib_timezone
grid_region = options.grid_region
grid_source = options.grid_source
local_timezone = options.local_timezone
utc_file = options.utc_file
verbose = options.verbose or debug

if utc_file: file_timezone = 'UTC'
else: file_timezone = local_timezone

variable = args[0].upper()

if options.next_month:
    today = datetime.date.today()
    if today.month < 12:
        year = today.year
        month = today.month + 1
    else:
        year = today.year + 1
        month = 1
    date_tuple = (year, month, 1)
else:
    num_args = len(args)
    if len(args) == 1:
        today = datetime.date.today()
        date_tuple = (today.year, today.month, today.day)
    elif num_args == 4:
        date_tuple = tuple([int(t) for t in args[1:4]])
    else:
        errmsg = 'No arguments passed to script. You must at least specify'
        raise RuntimeError, '%s the grib variable name.' % errmsg

forecast_date = datetime.date(*date_tuple)

if verbose:
    print 'requesting ...'
    print '      variable :', variable
    print ' forecast date :', forecast_date
    print 'local timezone :', local_timezone
    print ' file timezone :', file_timezone

# create a factory for access to grid files
factory = NdfdGridFileFactory(CONFIG, file_timezone=file_timezone)
if dev_mode: factory.useDirpathsForMode('dev')
region = factory.regionConfig(grid_region)
source = factory.sourceConfig(grid_source)

# look for overrides of the default timespan parameters
kwargs = { 'timezone':file_timezone, }

if verbose:
    grid_start_time, grid_end_time = factory.monthTimespan(forecast_date)
    num_hours = diffInHours(grid_end_time, grid_start_time, inclusive=True)
    print '\nexpected grid file timespan in %s timeszone :' % file_timezone
    print '     start hour :', grid_start_time
    print '       end hour :', grid_end_time
    print '      num hours :', num_hours

# build the file for the time span that includes the forecast date
factory.buildForecastGridFile(forecast_date, variable, region=grid_region, 
                              source='ndfd.grid', timezone=file_timezone,
                              verbose=verbose, debug=debug)

print '\ncompleted build for "%s" grid file.' % variable

