#! /Volumes/Transport/venv2/ndfd/bin/python

import os, sys
import datetime

from atmosci.utils.timeutils import diffInHours
from atmosci.ndfd.factory import NdfdGridFileFactory

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from atmosci.ndfd.config import CONFIG

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()

parser.add_option('-r', action='store', dest='grid_region',
                        default=CONFIG.sources.ndfd.grid.default_region)
parser.add_option('-s', action='store', dest='grid_source',
                        default=CONFIG.sources.ndfd.grid.default_source)

parser.add_option('-d', action='store_true', dest='dev_mode', default=False)
parser.add_option('-n', action='store_true', dest='next_month', default=False)
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
grib_region = options.grib_region.upper()
grib_timezone = options.grib_timezone
grid_region = options.grid_region
grid_source = options.grid_source
local_timezone = options.local_timezone
next_month = options.next_month
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
    date_args = args[1:]
    num_args = len(date_args) - 1
    if num_args == 0:
        today = datetime.date.today()
        date_tuple = (today.year, today.month, today.day)
    elif num_args == 1:
        date_tuple = (date_args[0], 1, 1)
    elif len(args) == 2:
        date_tuple = (date_args[0], date_args[1], 1)
    else :
        date_tuple = tuple([int(t) for t in date_args])
        if len(date_tuple) > 3: date_tuple = date_tuple[0:4]


forecast_date = datetime.date(*[int(d) for d in date_tuple])
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
print source

filepath = \
    factory.ndfdGridFilepath(forecast_date, variable, region, source, make_grid_dirs=False)
print filepath
exit()

if os.path.exists(filepath):
    # file already exists
    response = 'no response given yet'
    while not response in ('yes','no'):
        print "\ngrid file for already exists :"
        print "    ", filepath
        print "do you want to replace it ?" 
        response = raw_input("Enter 'yes' or 'no' : ")
        if response in ('y', 'yes'):
            os.remove(filepath)
            if os.path.exists(ndfd_filepath):
                print "Unable to delete grid file. Make sure permissions are properly set." 
            else: print "File was successfully deleted."
            break
        elif response in ('n', 'no'):
            print 'Execution terminate, will not replace current file.'
            exit(0) 

grid_dirpath, filename = os.path.split(filepath)
if not os.path.exists(grid_dirpath): os.makedirs(grid_dirpath)

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

