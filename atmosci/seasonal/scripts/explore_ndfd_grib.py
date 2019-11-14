#! /Volumes/Transport/venv2/ndfd/bin/python

import os, sys
import urllib

import datetime
from dateutil.relativedelta import relativedelta

import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot

import numpy as N
import pygrib

from atmosci.utils.options import stringToBbox
from atmosci.utils.timeutils import elapsedTime, asDatetime

from atmosci.seasonal.factory import NDFDProjectFactory

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

var_name_map = {'maxt':'Maximum temperature', 'mint':'Minimum temperature'}

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()

parser.add_option('-g', action='store', dest='variable', default='maxt')
parser.add_option('-o', action='store', type=float, dest='offset',
                        default=None)
parser.add_option('-r', action='store', dest='region', default=None)
parser.add_option('-s', action='store', dest='source', default=None)
parser.add_option('-t', action='store', dest='timespan', default='001-003')
parser.add_option('-v', action='store_true', dest='verbose', default=False)
parser.add_option('-z', action='store_true', dest='debug', default=False)

options, args = parser.parse_args()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

debug = options.debug
grib_variable = options.variable
time_span = options.timespan
verbise = options.verbose or debug

latest_time = datetime.datetime.utcnow()
target_year = latest_time.year

if len(args) == 0:
    fcast_date = datetime.date.today()
elif len(args) == 3:
    fcast_date = datetime.date(int(args[0]), int(args[1]), int(args[2]))
else:
    errmsg = 'Invalid number of command line arguments. Either pass None'
    errmsg += ' for current day or the complete year, month, day to explore.'
    SyntaxError, errmsg

factory = NDFDProjectFactory()
project = factory.getProjectConfig()
ndfd_config = factory.getSourceConfig('ndfd')

region_key = options.region
if region_key is None:
    region_key = factory.project.region
region = factory.getRegionConfig(region_key)
print 'region =', region.description

region_bbox = list(stringToBbox(region.data))
print 'project region bounding box', region_bbox

grib_filepath = factory.forecastGribFilepath(ndfd_config, fcast_date,
                                             time_span, grib_variable)
print '\nreading gribs from', grib_filepath

gribs = pygrib.open(grib_filepath)
#for grib in gribs.read():
#    print '\n', grib, '\n', grib.keys()
print 'exploring grib variable "%s"' % grib_variable
grib = gribs.select(name=var_name_map[grib_variable])
print 'grib\n', grib

for message_num in range(len(grib)):
    print '\n\n    message number', message_num
    message = grib[message_num]
    print '    "name" =', message.name
    print '    "analDate" =', message.analDate
    print '    "forecastTime" =', message.forecastTime
    print '    "validDate" =', message.validDate
    print '    "validityTime" =', message.validityTime
    print '    "dataDate" =', message.dataDate
    print '    "dataTime" =', message.dataTime
    print '    "month" =', message.month
    print '    "day" =', message.day
    print '    "hour" =', message.hour
    lat, lon = message.latlons()
    print '    lat stats:', lat.shape, lat.min(), lat.max()
    print '    lon stats:', lon.shape, lon.min(), lon.max()
    values = message.values
    print '    value stats :', values.shape, values.min(), values.max()

gribs.close()

