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

VARNAME_MAP = { 'maxt':'Maximum temperature',
                'mint':'Minimum temperature' }

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def nearestNode(target_lon, target_lat, grid_lons, grid_lats, radius, debug):
    indexes = N.where( (grid_lons >= (target_lon - radius)) &
                       (grid_lons <= (target_lon + radius)) &
                       (grid_lats >= (target_lat - radius)) &
                       (grid_lats <= (target_lat + radius)) )
    if len(indexes[0]) < 1:
        if target_lon <= -120: # target is in southwest corner of source grid
            indexes = N.where( (grid_lons < -120) & (grid_lats < 36) )
        else: # target is in southeast corner of source grid
            indexes = N.where( (grid_lons > -70) & (grid_lats < 36) )
        if len(indexes[0]) < 1:
            errmsg = 'Unable to match location %.5f, %.5f to any point in grib'
            raise IndexError, errmsg % (target_lat,target_lon)
    if debug:
        print '\ntarget lon', target_lon
        print 'lon subset\n', grid_lons[indexes]
    lon_diffs = N.absolute(grid_lons[indexes] - target_lon)
    if debug:
        print 'target lat', target_lat
        print 'lat subset\n', grid_lats[indexes]
    lat_diffs = grid_lats[indexes] - target_lat
    distances = N.sqrt( (lon_diffs * lon_diffs) + (lat_diffs * lat_diffs) )
    if debug: print 'distances', distances
    indx = N.where(distances == distances.min())[0][0]
    min_distance = distances[indx]
    if debug: print 'distance at index %d = %.5f' % (indx, min_distance)
    y = indexes[0][indx] 
    x = indexes[1][indx]
    return y, x, grid_lons[y,x], grid_lats[y,x], min_distance

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()

parser.add_option('-c', action='store', dest='csv_filepath', default=None)
parser.add_option('-g', action='store', dest='grib_variable', default='maxt')
parser.add_option('-m', action='store', type=int, dest='message',
                        default=0)
parser.add_option('-r', action='store', dest='region', default=None)
parser.add_option('-s', action='store', dest='source', default=None)
parser.add_option('-t', action='store', dest='timespan', default='001-003')
parser.add_option('-v', action='store_true', dest='verbose', default=False)
parser.add_option('-z', action='store_true', dest='debug', default=False)

options, args = parser.parse_args()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

debug = options.debug
grib_variable = options.grib_variable
message_num = options.message
verbose = options.verbose or debug

fcast_date = datetime.date(int(args[0]), int(args[1]), int(args[2]))
print '\nIndexing forecast for', fcast_date

factory = NDFDProjectFactory()
project = factory.getProjectConfig()

csv_filepath = options.csv_filepath
if csv_filepath is not None:
    csv_filepath = os.path.abspath(csv_filepath)

region_key = options.region
if region_key is None:
    region_key = factory.project.region
region = factory.getRegionConfig(region_key)
print 'region =', region.description
region_bbox = list(stringToBbox(region.data))
print 'project region bounding box', region_bbox

source_key = options.source
if source_key is None:
    source_key = factory.project.source
source = factory.getSourceConfig(source_key)

ndfd_config = factory.getSourceConfig('ndfd')

# all regions other than conus need a subset of the forecast area
if source_key != 'conus':
    ndfd_bbox = ndfd_config.get('bbox.%s' % region_key, None)
    if ndfd_bbox is not None:
        ndfd_bbox = stringToBbox(ndfd_bbox)
    else:
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
        ndfd_bbox = [ ]
        ndfd_bbox.append(region_bbox[0] - lon_offset)
        ndfd_bbox.append(region_bbox[1] - lat_offset)
        ndfd_bbox.append(region_bbox[2] + lon_offset)
        ndfd_bbox.append(region_bbox[3] + lat_offset)
    print 'NDFD bounding box', ndfd_bbox

# conus region needs the entire forecast area
else: ndfd_bbox = None

grib_filepath = factory.forecastGribFilepath(ndfd_config, fcast_date, 
                                             options.timespan, grib_variable)
print '\nreading gribs from', grib_filepath

gribs = pygrib.open(grib_filepath)
grib = gribs.select(name=VARNAME_MAP[grib_variable])
print '\n', grib
print '\n grib %d :' % message_num, grib_variable
message = grib[message_num]
print '    "dataDate"', message.dataDate
fcast_hour = message['hour']
print '\n    "forecast hour"', fcast_hour
print '    "forecastTime"', message['forecastTime']
fcast_time = asDatetime(message.dataDate) + relativedelta(hours=fcast_hour)
print '    forecast datetime', fcast_time
ndfd_lats, ndfd_lons = message.latlons()
print '\nfrom grib file :'
print '    grib lat', ndfd_lats.shape, ndfd_lats.min(), ndfd_lats.max()
lat_diff = ndfd_lats[1:,:] - ndfd_lats[:-1,:]
print '    min lat diff', lat_diff.min()
max_lat_diff = lat_diff.max()
print '    max lat diff', max_lat_diff
print '\n    grib lon', ndfd_lons.shape, ndfd_lons.min(), ndfd_lons.max()
lon_diff = ndfd_lons[:,1:] - ndfd_lons[:,:-1]
print '    min lon diff', lon_diff.min()
max_lon_diff = lon_diff.max()
print '    max lon diff', max_lon_diff 
max_spacing = max(max_lon_diff, max_lat_diff)
print '\n    max node spacing', max_spacing
print '    avg node spacing', lat_diff.mean(), lon_diff.mean()
search_radius = max_spacing * 1.25
print '    search radius', search_radius

if region.name == 'conus':
    min_y = 0
    max_y = ndfd_lats.shape[0]
    region_lats = ndfd_lats
    min_x = 0
    max_x = ndfd_lons.shape[1]
    region_lons = ndfd_lons
else:
    indexes = N.where(((ndfd_lats > ndfd_bbox[1]) & (ndfd_lats < ndfd_bbox[3])) &
                      ((ndfd_lons > ndfd_bbox[0]) & (ndfd_lons < ndfd_bbox[2])))
    min_y, max_y = indexes[0].min(), indexes[0].max()
    min_x, max_x = indexes[1].min(), indexes[1].max()

    region_lats = ndfd_lats[min_y:max_y+1, min_x:max_x+1]
    region_lons = ndfd_lons[min_y:max_y+1, min_x:max_x+1]

ndfd_y_offset = min_y
print '\n    grib min/max y index', min_y, max_y
after_offset = (min_y-ndfd_y_offset, max_y-ndfd_y_offset)
print '    grib y offset', ndfd_y_offset, after_offset
ndfd_x_offset = min_x
print '    grib min/max x index', min_x, max_x
after_offset = (min_x-ndfd_x_offset, max_x-ndfd_x_offset)
print '    grib x offset', ndfd_x_offset, after_offset
print '    region lat', region_lats.shape, region_lats.min(), region_lats.max()
print '    region lon', region_lons.shape, region_lons.min(), region_lons.max()

# get a static file manager for the target year
static_mgr = factory.getStaticFileManager(source_key, region, mode='r')
print '\nstatic file :', static_mgr.filepath
# get static ACIS/PRISM lat, lon
lats = static_mgr.lats
lons = static_mgr.lons
static_mgr.close()
print '\ncoord shape', lats.shape

index_start_time = datetime.datetime.now()
ndfd_x = [ ]
ndfd_y = [ ]
num_rows, num_cols = lats.shape
print 'source dimensions', num_rows, num_cols
status = 'completed row %d at %s'

if csv_filepath:
    print 'saving output to csv file', csv_filepath
    csv_file = open(csv_filepath, 'w')
    csv_file.write('ROW, COL, NDFD Y, NDFD X, lat, NDFD lat, lon, NDFD lon, distance')
    fmt = '\n %4i, %4i, %4i, %4i, %.5f, %.5f, %.5f, %.5f, %.5f'

distance = [ ]
region_lats = [ ]
region_lons = [ ]
for row in range(num_rows):
    row_dist = [ ]
    row_lat = [ ]
    row_lon = [ ]
    row_x = [ ]
    row_y = [ ]
    for col in range(num_cols):
        lat = lats[row,col]
        lon = lons[row,col]
        y, x, ndfdlon, ndfdlat, dist = \
            nearestNode(lon, lat, ndfd_lons, ndfd_lats, search_radius, debug)
        row_dist.append(dist)
        row_lat.append(ndfdlat)
        row_lon.append(ndfdlon)
        row_x.append(x)
        row_y.append(y)
        if debug: print row, col, y, x, lat, ndfdlat, lon, ndfdlon, dist
        if csv_filepath:
            line = fmt % (row, col, y, x, lat, ndfdlat, lon, ndfdlon, dist)
            csv_file.write(line)
    if verbose: print status % (row, elapsedTime(index_start_time, True))

    distance.append(row_dist)
    ndfd_x.append(row_x)
    ndfd_y.append(row_y)
    region_lats.append(row_lat)
    region_lons.append(row_lon)

elapsed_time = elapsedTime(index_start_time, True)
print 'completed indexing %d rows in %s' % (num_rows,elapsed_time)

del lats, lons, ndfd_lats, ndfd_lons

if csv_filepath:
    csv_file.close()

distance = N.array(distance)
region_lats = N.array(region_lats)
region_lons = N.array(region_lons)
ndfd_x = N.array(ndfd_x)
ndfd_y = N.array(ndfd_y)

dirpath, filename = os.path.split(grib_filepath)
date_dir = dirpath[dirpath.rfind(os.sep)+1:]
filepath = os.path.join(date_dir, filename)
static_mgr.open('a')
if not static_mgr.hasGroup('ndfd'):
    static_mgr.createGroup('ndfd',
            description='Mapping of NDFD CONUS grib to %s grid' % source.tag,
            source='National Digital Forecast Model', source_file=filepath,
            min_indexes=(min_y,min_x), max_indexes=(max_y,max_x),
            region=region.description, bbox=ndfd_config.bbox[region.name],
            )
static_mgr.setGroupAttribute('ndfd', 'source_file', filepath)
static_mgr.close()

static_mgr.open('a')
if verbose:
    print 'groups in file', static_mgr.listGroups()
    print 'datasets in file', static_mgr.listDatasets()
static_mgr.createDataset('ndfd.y_indexes', ndfd_y, offset=ndfd_y_offset,
        description='Y index for equivalent node in NDFD 2.5k CONUS grib',
        region=region.description)
static_mgr.close()

static_mgr.open('a')
static_mgr.createDataset('ndfd.x_indexes', ndfd_x, offset=ndfd_x_offset,
           description='X index for equivalent node in NDFD 2.5k CONUS grib',
           region=region.description)
static_mgr.close()

description='distance in degrees b/w %s node and node in NDFD 2.5k CONUS grib'
static_mgr.open('a')
static_mgr.createDataset('ndfd.distance', distance, avg=distance.mean(),
                         max=distance.max(), min=distance.min(),
                         description=description % source.tag,
                         region=region.description)
static_mgr.close()

static_mgr.open('a')
static_mgr.createDataset('ndfd.lat', region_lats,
           max=region_lats.max(), min=region_lats.min(),
           description='Latitude for equivalent node in NDFD 2.5k CONUS grib',
           region=region.description)
static_mgr.close()

static_mgr.open('a')
static_mgr.createDataset('ndfd.lon', region_lons,
           max=region_lons.max(), min=region_lons.min(),
           description='Longitude for equivalent node in NDFD 2.5k CONUS grib',
           region=region.description)
static_mgr.close()

print '\ntotal elapsed time', elapsedTime(index_start_time, True)

