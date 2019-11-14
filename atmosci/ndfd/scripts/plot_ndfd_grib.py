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
parser.add_option('-m', action='store', type=int, dest='message',
                        default=0)
parser.add_option('-o', action='store', type=float, dest='offset',
                        default=None)
parser.add_option('-p', action='store_true', dest='plot', default=False)
parser.add_option('-r', action='store', dest='region', default=None)
parser.add_option('-s', action='store', dest='source', default=None)
parser.add_option('-t', action='store', dest='timespan', default='001-003')
parser.add_option('-v', action='store_true', dest='verbose', default=False)
parser.add_option('-z', action='store_true', dest='debug', default=False)

options, args = parser.parse_args()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

debug = options.debug
draw_plot = options.plot
message_num = options.message
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

region_key = options.region
if region_key is None:
    region_key = factory.project.region
region = factory.getRegionConfig(region_key)
print 'region =', region.description

region_bbox = list(stringToBbox(region.data))
print 'project region bounding box', region_bbox

ndfd_config = factory.getSourceConfig('ndfd')
offset = options.offset
if offset is None:
    offset = ndfd_config.bbox_offset
    if isinstance(offset, (ndfd_config.__class__, dict)):
        lat_offset = offset.lat
        lon_offset = offset.lon
    elif isinstance(offset, (tuple,list)):
        if offset[0] < 0:
            lon_offset, lat_offset = offset
        else:
            lat_offset, lon_offset = offset
    else:
        lat_offset = lon_offset = offset
print 'using lat offset =', lat_offset
print 'using lon offset =', lon_offset

grib_filepath = factory.forecastGribFilepath(ndfd_config, fcast_date,
                                             time_span, grib_variable)
print '\nreading gribs from', grib_filepath

gribs = pygrib.open(grib_filepath)
ndfd_bbox = [ ]
ndfd_bbox.append(region_bbox[0] - lon_offset)
ndfd_bbox.append(region_bbox[1] - lat_offset)
ndfd_bbox.append(region_bbox[2] + lon_offset)
ndfd_bbox.append(region_bbox[3] + lat_offset)
print 'NDFD bounding box', ndfd_bbox
#for grib in gribs.read():
#    print '\n', grib, '\n', grib.keys()
print 'exploring grib variable "%s"' % grib_variable
grib = gribs.select(name=var_name_map[grib_variable])
print '\n', grib
print '\nin grib 0 :'
message = grib[message_num]
print '    "dataDate"', message.dataDate
fcast_hour = message['hour']
print '    "fcast_hour"', fcast_hour
print '    "forecastTime"', message['forecastTime']
fcast_time = asDatetime(message.dataDate) + relativedelta(hours=fcast_hour)
print 'forecast datetime', fcast_time
lat, lon = message.latlons()
print '    grib lat', lat.shape, lat.min(), lat.max()
print '    grib lon', lon.shape, lon.min(), lon.max()
maxt = message.values
print '    grib maxt', maxt.shape, maxt.min(), maxt.max()

if region.name == 'conus':
    ndfd_lat = lat
    ndfd_lon = lon
    ndfd_maxt = maxt
else:
    indexes = N.where( ((lat > ndfd_bbox[1]) & (lat < ndfd_bbox[3])) &
                       ((lon > ndfd_bbox[0]) & (lon < ndfd_bbox[2])) )
    min_y, max_y = indexes[0].min(), indexes[0].max()
    print '    grib y index limits', min_y, max_y
    min_x, max_x = indexes[1].min(), indexes[1].max()
    print '    grib x index limits', min_x, max_x

    ndfd_lat = lat[min_y:max_y, min_x:max_x]
    print '    region lat', ndfd_lat.shape, ndfd_lat.min(), ndfd_lat.max()
    ndfd_lon = lon[min_y:max_y, min_x:max_x]
    print '    region lon', ndfd_lon.shape, ndfd_lon.min(), ndfd_lon.max()
    ndfd_maxt = maxt[min_y:max_y, min_x:max_x]
    print '    region maxt', ndfd_maxt.shape, ndfd_maxt.min(), ndfd_maxt.max()

mid_x_indx = ndfd_lat.shape[1] / 2
mid_y_indx = ndfd_lat.shape[0] / 2

print ' '
print 'first row'
print ndfd_lat[0,0] , ndfd_lon[0,0]
print ndfd_lat[0,mid_x_indx] , ndfd_lon[0,mid_x_indx]
print ndfd_lat[0,-1] , ndfd_lon[0,-1]
print 'midpoint', mid_y_indx, mid_x_indx
print ndfd_lat[mid_y_indx,0] , ndfd_lon[mid_x_indx,0]
print ndfd_lat[mid_y_indx,mid_x_indx] , ndfd_lon[mid_y_indx,mid_x_indx]
print ndfd_lat[mid_y_indx,-1] , ndfd_lon[mid_y_indx,-1]
print 'last row'
print ndfd_lat[-1,0] , ndfd_lon[-1,0]
print ndfd_lat[-1,mid_y_indx] , ndfd_lon[-1,mid_x_indx]
print ndfd_lat[-1,-1] , ndfd_lon[-1,-1]
print ' '
if draw_plot:
    figure = pyplot.figure(figsize=(8,6), dpi=100)
    axes = figure.gca()
    pyplot.plot(ndfd_lon[0], ndfd_lat[0], c='b', label='NDFD')
    pyplot.plot(ndfd_lon[mid_y_indx], ndfd_lat[mid_y_indx], c='b')
    pyplot.plot(ndfd_lon[-1], ndfd_lat[-1], c='b')
    pyplot.plot(ndfd_lon[:,0], ndfd_lat[:,0], c='b')
    pyplot.plot(ndfd_lon[:,-1], ndfd_lat[:,-1], c='b')

    pyplot.plot([region_bbox[0], region_bbox[2]],
                [region_bbox[1],region_bbox[1]], c='r', label='ACIS/PRISM')
    pyplot.plot([region_bbox[2], region_bbox[2]],
                [region_bbox[1],region_bbox[3]], c='r')
    pyplot.plot([region_bbox[2], region_bbox[0]],
                [region_bbox[3],region_bbox[3]], c='r')
    pyplot.plot([region_bbox[0], region_bbox[0]],
                 [region_bbox[3],region_bbox[1]], c='r')

    if region.name == 'conus':
        pyplot.xlim(-132., -59.)
        pyplot.ylim(20., 54.)
    else:
        pyplot.xlim(-86., -63.)
        pyplot.ylim(34., 51.)
    axes.grid(True)
    axes.set_xlabel('Longitude', fontsize=12)
    axes.set_ylabel('Latitude', fontsize=12)
    pyplot.legend(prop={'size':10}, fancybox=True, framealpha=0.5,
                  loc='center')

    pyplot.axes(axes)
    pyplot.suptitle('%s (offset=%.3f)' % (region.name.upper(), offset),
                    fontsize=12)
    figure.savefig('grid_outlines_%s.png' % region.name)

print '\n\n'
keys = message.keys()
keys.sort()
for key in keys:
    if key == 'analDate':
        print 'analDate =', message.analDate
    elif key == 'codedValues':
        print 'codedValues = data values as encoded infile'
    elif key == 'validDate':
        print 'validDate =', message.validDate
    elif key == 'values':
        print 'values = data values after transforms applied (real data)'
    else: print key, '=', message[key]

gribs.close()

