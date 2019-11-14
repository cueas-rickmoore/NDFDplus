#! /Volumes/Transport/venv2/ndfd/bin/python

import datetime
import exceptions, warnings
import os
START_TIME = datetime.datetime.now()

# WARNING generated when cfgrib is imported
# /Volumes/Transport/venv2/ndfd/lib/python2.7/site-packages/distributed/utils.py:136:
# RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8',
# defaulting to '127.0.0.1': [Errno 51] Network is unreachable
# RuntimeWarning,
warnings.filterwarnings('ignore', "Couldn't detect a suitable IP address for reaching", exceptions.RuntimeWarning, 'distributed', 136)
warnings.filterwarnings('ignore', "dataset.value has been deprecated", exceptions.DeprecationWarning, 'h5py', 313)

import numpy as N
#from atmosci.seasonal.registry import REGBASE
from atmosci.seasonal.factory import SeasonalStaticFileFactory
from atmosci.utils.timeutils import elapsedTime

from atmosci.ndfd.config import CONFIG
from atmosci.ndfd.factory import NdfdStaticFileFactory
#from atmosci.ndfd.grib import NdfdGribNodeFinder
from atmosci.ndfd.static import NdfdStaticGridFileBuilder

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# Usage :
#    create_ndfd_static_region_file.py region [yyyy mm dd] [-t --gribvar --timespan] [-d -v -x -z]
#    yyyy mm dd = date of grib file to use for retrieving lat/lon grids
#    if date is not specified, script will look for grib file for the current day
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()

parser.add_option('-d', action='store_true', dest='dev_mode', default=False)
parser.add_option('-v', action='store_true', dest='verbose', default=False)
parser.add_option('-x', action='store_true', dest='extreme_debug', default=False)
parser.add_option('-z', action='store_true', dest='debug', default=False)

parser.add_option('-t', action='store', type=float, dest='tolerance', default=0.1)
parser.add_option('--gribvar', action='store', type=str, dest='gribvar',
                               default='maxt')
parser.add_option('--timespan', action='store', type=str, dest='timespan',
                                default='001-003')

options, args = parser.parse_args()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

dev_mode = options.dev_mode
gribvar = options.gribvar
timespan = options.timespan

extreme_debug = options.extreme_debug
debug = options.debug or extreme_debug
verbose = options.verbose or debug

source = CONFIG.static.ndfd
# coordinate distance difference tolerance, default tolerance = 0.1
tolerance = options.tolerance

region = args[0].upper()
print 'building static file for %s region with corresponding NDFD location data' % region
region_path = 'regions.%s.data' % region
if debug:
    print 'region_path =', region_path
    print 'lat/lon tolerance =', tolerance
    print '\n', CONFIG.regions[region], '\n'

num_date_args = len(args) - 1
if num_date_args == 0:
    fcast_date = datetime.date.today()
elif num_date_args == 3:
    date = [int(arg) for arg in args[1:4]]
    fcast_date = datetime.date(date[0], date[1], date[2])
else:
    errmsg = 'Invalid date passed to script.\nYear, Month & Day are required.'
    raise RuntimeError, errmsg

static_factory = SeasonalStaticFileFactory(CONFIG)
static_factory.useDirpathsForMode('dev')
conus_filepath = static_factory.staticGridFilepath('acis5k', 'conus')
del static_factory

ndfd_static_factory = NdfdStaticFileFactory(CONFIG)
if dev_mode: ndfd_static_factory.useDirpathsForMode('dev')
ndfd_filepath = ndfd_static_factory.ndfdStaticFilepath(source, region)
if os.path.exists(ndfd_filepath):
    response = 'no response given yet'
    while not response in ('yes','no'):
        print "\nstatic file for %s region already exists, do you want to replace it ?" % region
        response = raw_input("Enter 'yes' or 'no' : ")
        if response in ('y', 'yes'):
            os.remove(ndfd_filepath)
            print ndfd_filepath, 'exists :', os.path.exists(ndfd_filepath)
            break
        elif response in ('n', 'no'):
            print 'Execution ending, will not replace', ndfd_filepath
            exit()

builder = NdfdStaticGridFileBuilder(conus_filepath, ndfd_filepath, CONFIG, region, tolerance=tolerance, debug=debug)

with warnings.catch_warnings():
    print '\nBuilding Acis datasets'
    builder.buildAcisDatasets(debug=debug)

    print '\nBuilding NDFD group datasets datasets'
    builder.buildNdfdGroup(debug=debug)

builder.close()

elapsed_time = elapsedTime(START_TIME, True)
fmt = 'completed creation of NDFD static file for %s in %s' 
print fmt % (region, elapsed_time)
print 'filepath :', builder.filepath
