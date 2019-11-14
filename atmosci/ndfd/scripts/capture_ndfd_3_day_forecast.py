#! /Volumes/Transport/venv2/ndfd/bin/python

import os, sys
import urllib

import datetime
from dateutil.relativedelta import relativedelta

import numpy as N
import pygrib

from atmosci.utils.options import stringToBbox
from atmosci.utils.timeutils import elapsedTime, asDatetime

from atmosci.seasonal.factory import NDFDProjectFactory

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()

parser.add_option('-n', action='store', type=int, dest='num_hours', default=72)
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
num_hours = options.num_hours
region_key = options.region
source_key = options.source
timespan = options.timespan
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

factory = NDFDProjectFactory()
if dev_mode: factory.useDirpathsForMode('dev')
project = factory.getProjectConfig()
ndfd_config = factory.getSourceConfig('ndfd')

if region_key is None:
    region_key = factory.project.region
grid_region = factory.regionConfig(region_key)
print 'region =', grid_region.description

if source_key is None:
    source_key = factory.project.source
grid_source = factory.sourceConfig(source_key)
print 'source =', grid_source.description

reader = factory.staticFileReader(grid_source, grid_region)
grid_shape, grib_indexes = reader.gribSourceIndexes('ndfd')
data_mask = reader.getData('cus_mask')
reader.close()

grid = N.empty((num_hours,)+grid_shape, dtype=float)
grid.fill(N.nan)

grib_times = [ ]

grib_filepath = factory.forecastGribFilepath(factory.sourceConfig('ndfd'),
                                             fcast_date, timespan, variable)
print '\nreading gribs from', grib_filepath

gribs = pygrib.open(grib_filepath)
for grib in gribs.select():
    index = grib.forecastTime - 1
    missing_value = float(grib.missingValue)
    grib_time = grib.validDate

    values = grib.values[grib_indexes].reshape(grid_shape)
    if N.ma.is_masked(values): values = values.data
    values[N.where(values >= missing_value)] = N.nan
    values[N.where(data_mask == True)] = N.nan
    grid[index,:,:] = values
    grib_times.append((index, grib_time))

units = grib.units
gribs.close()

print '\n\n times\n', grib_times
print '\n\n grid', grid.shape, N.nanmin(grid), N.nanmax(grid)
print grid

# fill in the gaps
adjusted_index = None
prev_index, prev_hour = grib_times[0]
for index in range(1,len(grib_times)):
    data_index, data_hour = grib_times[index]
    diff = relativedelta(data_hour, prev_hour).hours
    if diff > 1:
        print '\n @', data_index, ':', diff, 'missing hours :', prev_hour, data_hour
        if adjusted_index is None: adjusted_index = prev_index

        prev_values = grid[prev_index,:,:]
        data_values = grid[data_index,:,:]
        adjustment = (data_values - prev_values) / (diff + 1)
        for n in range(1, diff+1):
            next_index = prev_index + n
            print 'updating', prev_hour + relativedelta(hours=n)
            grid[next_index, :, :] = data_values + (adjustment * n)

    prev_hour = data_hour
    prev_index = data_index

if adjusted_index is not None:
    subset = grid[adjusted_index:,:,:]
    print '\n\n adjusted subset', N.nanmin(subset), N.nanmax(subset)
    print subset

