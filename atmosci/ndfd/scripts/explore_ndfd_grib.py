#! /Volumes/Transport/venv2/ndfd/bin/python

import os, sys
import urllib

import datetime
from dateutil.relativedelta import relativedelta

import numpy as N
import pygrib

from atmosci.utils.options import stringToBbox
from atmosci.utils.timeutils import elapsedTime, asDatetime

from atmosci.ndfd.factory import NdfdGribFileFactory

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()

parser.add_option('-o', action='store', type=float, dest='offset',
                        default=None)
parser.add_option('-r', action='store', dest='region', default=None)
parser.add_option('-s', action='store', dest='source', default=None)
parser.add_option('-t', action='store', dest='timespan', default='001-003')

parser.add_option('-d', action='store_true', dest='dev_mode', default=False)
parser.add_option('-i', action='store_true', dest='inventory', default=False)
parser.add_option('-v', action='store_true', dest='verbose', default=False)
parser.add_option('-z', action='store_true', dest='debug', default=False)

options, args = parser.parse_args()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

debug = options.debug
dev_mode = options.dev_mode
inventory = options.inventory
time_span = options.timespan
verbose = options.verbose or debug

latest_time = datetime.datetime.utcnow()
target_year = latest_time.year

variable = args[0]

if len(args) == 1:
    fcast_date = datetime.date.today()
elif len(args) == 4:
    fcast_date = datetime.date(int(args[1]), int(args[2]), int(args[3]))
else:
    errmsg = 'Invalid number of command line arguments. Either pass None'
    errmsg += ' for current day or the complete year, month, day to explore.'
    SyntaxError, errmsg

factory = NdfdGribFileFactory()
if dev_mode: factory.useDirpathsForMode('dev')
project = factory.getProjectConfig()
ndfd_config = factory.getSourceConfig('ndfd')

region_key = options.region
if region_key is None:
    region_key = factory.project.region
region = factory.getRegionConfig(region_key)
print 'region =', region.description

region_bbox = list(stringToBbox(region.data))
print 'project region bounding box', region_bbox

print 'exploring grib variable "%s"' % variable
grib_filepath = factory.ndfdGribFilepath(fcast_date, variable, time_span)
print '\nreading gribs from', grib_filepath
gribs = pygrib.open(grib_filepath)

if inventory:
    print 'grib inventory :'
    #grib = gribs[1]
    #for key in sorted(grib.keys()):
    #    print '    ', key
    for grib in gribs.select():
        print grib.name, grib.forecastTime, grib.validDate
    exit()

for grib_num, grib in enumerate(gribs.select()):
    print '\n\ngrib number %d' % grib_num
    print '    name =', grib.name
    print '    shortName =', grib.shortName
    print '    identifier =', grib.identifier
    print '    paramID =', grib.paramId
    print '    analDate =', grib.analDate
    print '    forecastTime =', grib.forecastTime
    print '    validDate =', grib.validDate
    print '    type validDate =', type(grib.validDate)
    print '    validityTime =', grib.validityTime
    print '    dataDate =', grib.dataDate
    print '    dataTime =', grib.dataTime
    print '    month =', grib.month
    print '    day =', grib.day
    print '    hour =', grib.hour
    lat, lon = grib.latlons()
    print '    lat stats:', lat.shape, lat.min(), lat.max()
    print '    lon stats:', lon.shape, lon.min(), lon.max()
    print '\n    gridType =', grib.gridType
    print '    packingType =', grib.packingType
    missing = grib.missingValue
    print '    missingValue =', missing
    print '    units =', grib.units
    values = grib.values
    values[N.where(values == missing)] = N.nan
    print '    value stats :', values.shape, N.nanmin(values), N.nanmax(values)

gribs.close()

