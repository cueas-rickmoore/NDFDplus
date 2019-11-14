#! /Volumes/Transport/venv2/ndfd/bin/python

import os, sys
import subprocess, shlex
import warnings

import datetime
UPDATE_START_TIME = datetime.datetime.now()

import numpy as N

from atmosci.utils.timeutils import elapsedTime
from atmosci.utils.units import convertUnits

from atmosci.ndfd.factory import NdfdGridFileFactory
from atmosci.ndfd.smart_grib import SmartNdfdGribFileReader


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from atmosci.ndfd.config import CONFIG


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()

parser.add_option('-r', action='store', dest='grid_region',
                        default=CONFIG.sources.ndfd.grid.region)
parser.add_option('-s', action='store', dest='grid_source',
                        default=CONFIG.sources.ndfd.grid.source)
parser.add_option('-t', action='store', dest='timespan', default=None)

parser.add_option('-d', action='store_true', dest='dev_mode', default=False)
parser.add_option('-g', action='store_true', dest='graceful_fail',
                        default=False)
parser.add_option('-f', action='store_true', dest='fill_gaps', default=False)
parser.add_option('-u', action='store_false', dest='utc_file', default=True)
parser.add_option('-v', action='store_true', dest='verbose', default=False)
parser.add_option('-z', action='store_true', dest='debug', default=False)

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
fill_gaps = options.fill_gaps
file_timezone = options.file_timezone
graceful_fail = options.graceful_fail
grib_region_key = options.grib_region
grib_timezone = options.grib_timezone
grid_region_key = options.grid_region
grid_source_key = options.grid_source
local_timezone = options.local_timezone
timespan = options.timespan
today = datetime.date.today()
utc_file = options.utc_file
verbose = options.verbose or debug

if utc_file: file_timezone = 'UTC'
else: file_timezone = local_timezone
verbose = options.verbose or debug

variable = args[0]

if len(args) == 3:
    target_date = datetime.date(today.year, int(args[1]), int(args[2]))
elif len(args) == 4:
    target_date = datetime.date(int(args[1]), int(args[2]), int(args[3]))
else: target_date = today

smart_grib = SmartNdfdGribFileReader()
if dev_mode: smart_grib.useDirpathsForMode('dev')
ndfd = smart_grib.sourceConfig('ndfd')
grib_region = smart_grib.regionConfig(grib_region_key)

grid_factory = NdfdGridFileFactory()
if dev_mode: grid_factory.useDirpathsForMode('dev')
grid_region = grid_factory.regionConfig(grid_region_key)
grid_source = grid_factory.sourceConfig(grid_source_key)
grid_dataset = grid_factory.ndfdGridDatasetName(variable)

# filter annoying numpy warnings
warnings.filterwarnings('ignore',"All-NaN axis encountered")
warnings.filterwarnings('ignore',"All-NaN slice encountered")
warnings.filterwarnings('ignore',"invalid value encountered in greater")
warnings.filterwarnings('ignore',"invalid value encountered in less")
warnings.filterwarnings('ignore',"Mean of empty slice")
warnings.filterwarnings('ignore',"setting an item on a masked array which has a shared mask will not copy the mask and also change the original mask array in the future.")
# MUST ALSO TURN OFF WARNING FILTERS AT END OF SCRIPT !!!!!

if timespan is None:
    timespans = ('001-003','004-007')
else: timespans = (timespan,)

units, data = smart_grib.dataForRegion(target_date, variable, timespans,
                         grid_region, grid_source, fill_gaps, graceful_fail,
                         debug)

if len(data) == 0:
    print 'NO DATA AVAILABLE FOR %s %s' % (str(target_date), timespan)
    exit()

fcast_start = data[0][1]
if len(data) > 1:
    fcast_end = data[-1][1]
else: fcast_end = fcast_start

fcast_date = fcast_start.date()
filepath = grid_factory.ndfdGridFilepath(fcast_date, variable, grid_region)
if not os.path.exists(filepath):
    grid_factory.buildForecastGridFile(fcast_date, variable, region=grid_region)
manager = grid_factory.ndfdGridFileManager(fcast_date, variable, grid_region, 'r')
print '\nupdating grid file :', manager.filepath
grid_end_time = manager.timeAttribute(grid_dataset, 'end_time')
print 'dataset end_time :', grid_end_time 
manager.close()

for source, fcast_time, grid in data:
    if fcast_time > grid_end_time:
        manager.close()
        target_date = fcast_time.date()
        #filepath = \
        #    grid_factory.ndfdGridFilepath(target_date, variable, grid_region)
        if not os.path.exists(filepath):
            grid_factory.buildForecastGridFile(target_date, variable,
                                               region=grid_region)
        manager = \
            grid_factory.ndfdGridFileManager(target_date, variable, grid_region)
        print '\nupdating grid file :', manager.filepath
        grid_end_time = manager.timeAttribute(grid_dataset, 'end_time')
        print 'dataset end_time :', grid_end_time
        manager.close()

    if debug:
        print 'inserting :', fcast_time, source, N.nanmin(grid), N.nanmax(grid)
    manager.open('a')
    manager.updateForecast(grid_dataset, fcast_time, grid, source=source)
    manager.close()


# turn annoying numpy warnings back on
warnings.resetwarnings()

elapsed_time = elapsedTime(UPDATE_START_TIME, True)
msg = '\ncompleted NDFD %s forecast update for %s thru %s in %s'
print msg % (variable.upper(), fcast_start.strftime('%m-%d'),
             fcast_end.strftime('%m-%d, %Y'), elapsed_time)
