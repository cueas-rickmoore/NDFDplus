#!/usr/bin/env python

import os, sys
import datetime
BUILD_START_TIME = datetime.datetime.now()

from dateutil.relativedelta import relativedelta

import numpy as N

from atmosci.utils.string import tupleFromString
from atmosci.utils.timeutils import elapsedTime
from atmosci.seasonal.factory import SeasonalProjectFactory

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()

parser.add_option('-v', action='store_true', dest='verbose', default=False)
parser.add_option('-z', action='store_true', dest='debug', default=False)

options, args = parser.parse_args()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

debug = options.debug
verbose = options.verbose

factory = SeasonalProjectFactory()
project = factory.getProjectConfig()
if verbose:
    print 'project :\n', project

source_key = args[0]
source = factory.getSourceConfig(source_key)

region_key = args[1]
if len(region_key) == 2: region_key = region_key.upper()
region = factory.getRegionConfig(region_key)
bbox = region.data
if verbose:
    print '\nregion :\n', region
    print '\nbbox', bbox


reader = factory.getStaticFileReader(source, factory.getRegionConfig('conus'))
print 'reading from :', reader.filepath
# set lat/lon coordinate bounds - in order to find exactly 1 matching
# node for each corner of the bounding box, use a tight 0.009 degree
# search tolerance in stead of the source file's default search radius
reader.setCoordinateBounds(tupleFromString(bbox, float), 0.009)
if 'lon' in reader.dataset_names:
    lon_dsn = 'lon'
    lat_dsn = 'lat'
else:
    lon_dsn = 'lons'
    lat_dsn = 'lats'


builder = factory.getStaticFileBuilder(source, region, bbox=bbox)
print 'building file :', builder.filepath
#builder.initFileAttributes()

print '    creating %s and %s datasets' % (lat_dsn, lon_dsn)
builder.initLonLatData(reader.getDataInBounds(lon_dsn),
                       reader.getDataInBounds(lat_dsn))

for group_name in reader.group_names:
    print '    creating %s group' % group_name
    builder.open('a')
    builder.createGroup(group_name, **reader.getGroupAttributes(group_name))
    builder.close()

dataset_paths = list(reader.dataset_names)
del dataset_paths[dataset_paths.index(lat_dsn)]
del dataset_paths[dataset_paths.index(lon_dsn)]
if 'eor' not in region:
    dataset_paths = [ path for path in dataset_paths if 'eor' not in path ]

for dataset_path in dataset_paths:
    print '    creating %s dataset' % dataset_path
    data = reader.getDataInBounds(dataset_path)
    attributes = reader.getDatasetAttributes(dataset_path)
    if 'min' in attributes:
        if data.dtype.kind == 'f':
            attributes['max'] = N.nanmax(data)
            attributes['min'] = N.nanmin(data)
        elif data.dtype.kind == 'i':
            missing = attributes.get('mising', None)
            if missing is None:
                attributes['max'] = data.max()
                attributes['min'] = data.min()
            else:
                valid = data[N.where(data != missing)]
                attributes['max'] = valid.max()
                attributes['min'] = valid.min()
    if 'created' in attributes: del attributes['created']
    if 'updated' in attributes: del attributes['updated']

    builder.open('a')
    builder.createDataset(dataset_path, data)
    builder.setDatasetAttributes(dataset_path, **attributes)
    builder.close()

reader.close()

elapsed_time = elapsedTime(BUILD_START_TIME, True)
print 'completed build of %s static file in' % source.tag, elapsed_time

