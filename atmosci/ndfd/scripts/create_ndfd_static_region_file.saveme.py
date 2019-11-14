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
import xarray
# from atmosci.hdf5.file import Hdf5FileReader
# from atmosci.hdf5.grid import Hdf5GridFileReader
# from atmosci.seasonal.factory import SeasonalStaticFileFactory
from atmosci.seasonal.registry import REGBASE
from atmosci.seasonal.factory import NDFDProjectFactory
from atmosci.seasonal.factory import AcisProjectFactory
from atmosci.utils.timeutils import elapsedTime

from atmosci.ndfd.config import CONFIG
from atmosci.ndfd.factory import NdfdStaticFileFactory
from atmosci.ndfd.grib import NdfdGribNodeFinder
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

source = CONFIG.static.ndfd

ndfd_static_factory = NdfdStaticFileFactory(CONFIG)
if dev_mode: ndfd_static_factory.useDirpathsForMode('dev')
ndfdStaticFilepath = ndfd_static_factory.ndfdStaticFilepath(source, region)
if os.path.exists(ndfdStaticFilepath):
    response = 'no response given yet'
    while not response in ('yes','no'):
        print "\nstatic file for %s region already exists, do you want to replace it ?" % region
        response = raw_input("Enter 'yes' or 'no' : ")
        if response in ('y', 'yes'):
            os.remove(ndfdStaticFilepath)
            print ndfdStaticFilepath, 'exists :', os.path.exists(ndfdStaticFilepath)
            break
        elif response in ('n', 'no'):
            print 'Execution ending, will not replace', ndfdStaticFilepath
            exit()

ndfd_factory = NDFDProjectFactory()
ndfd_factory.useDirpathsForMode('dev')

bbox = N.array([float(coord) for coord in ndfd_factory.config[region_path].split(',')])
bbox_lats = bbox[N.where(bbox > 0.)]
bbox_lons = bbox[N.where(bbox < 0.)]
if verbose:
    print 'region lats =', bbox_lats
    print 'region lons =', bbox_lons

print '\nreading lat, lon and elevation from ACIS CONUS file'
static_factory = AcisProjectFactory(CONFIG)
static_factory.useDirpathsForMode('dev')
static_reader = static_factory.staticFileReader('acis5k', 'conus')
acis_lats = static_reader.get2DSlice('lat', bbox[0], bbox[2], bbox[1], bbox[3])
acis_lons = static_reader.get2DSlice('lon', bbox[0], bbox[2], bbox[1], bbox[3])
static_reader.close()
dataset_shape = acis_lats.shape

if verbose:
    print "acis lats shape :", acis_lats.shape
    print "acis lons shape :", acis_lons.shape

# create empty arrays to contain coordinates of NDFD nodes closest to each ACIS node
ndfd_lats = N.full(acis_lats.shape, N.nan)
ndfd_lons = N.full(acis_lons.shape, N.nan)
if verbose: 
    print "ndfd static lats shape :", ndfd_lats.shape
    print "ndfd static lons shape :", ndfd_lons.shape

# arrays to track x,y indexes for nodes in NDFD coordinate datasets
distance = N.zeros(acis_lats.shape, dtype=float)
x_indexes = N.zeros(acis_lats.shape, dtype="<i2")
y_indexes = N.zeros(acis_lats.shape, dtype="<i2")

# get lon/lat from an existisng CONUS NDFD grib file

ndfd_filepath = ndfd_factory.forecastGribFilepath(None, fcast_date, timespan,gribvar)
print '\nreading latitude, longitude from NDFD grib file\n', ndfd_filepath
# open grib file using cfgrib directly ... a little flaky
# ds = cfgrib.open_file("/Volumes/Transport/data/app_data/shared/forecast/ndfd/20190901/001-003-maxt.grib")
# open grib file with xarray ... access thru cfgrib with xarray context
ds = xarray.open_dataset(ndfd_filepath, engine='cfgrib')
grib_lats = ds.variables['latitude'].data
grib_lons = ds.variables['longitude'].data
ds.close() # this doesn't work with cfgrib alone
neg_lons = grib_lons - 360.

if extreme_debug:
    print "grib_lats :", grib_lats[0,0], grib_lats[0,-1]
    print "grib_lats :", grib_lats[-1,0], grib_lats[-1,-1]
    print "grib_lons :", grib_lons[0,0], grib_lons[0,-1]
    print "grib_lons :", grib_lons[-1,0], grib_lons[-1,-1]
    print " neg_lons :", neg_lons[0,0], neg_lons[0,-1]
    print " neg_lons :", neg_lons[-1,0], neg_lons[-1,-1]
if debug:
    print 'bbox properties :'
    print '    ', bbox[0], bbox[2], bbox[1], bbox[3]
    print '    ', bbox[0] - tolerance, bbox[2] + tolerance, bbox[1] - tolerance, bbox[3] + tolerance

grib_indexes = N.where( (neg_lons >= (bbox[0] - tolerance)) &
                        (neg_lons <= (bbox[2] + tolerance)) &
                        (grib_lats >= (bbox[1] - tolerance)) &
                        (grib_lats <= (bbox[3] + tolerance))
                      )
if extreme_debug:
    print 'grib_indexes[0].min =', grib_indexes[0].min() 
    print 'grib_indexes[0].max =', grib_indexes[0].max() 
    print 'grib_indexes[0].len =', len(grib_indexes[0]) 
    print 'grib_indexes[1].min =', grib_indexes[1].min()
    print 'grib_indexes[1].max =', grib_indexes[1].max()
    print 'grib_indexes[1].len =', len(grib_indexes[1])
    print 'neg_lons[grib_indexes].shape :', neg_lons[grib_indexes].shape
if debug:
    print '\nlons in bbox :', neg_lons[grib_indexes].max(), neg_lons[grib_indexes].min()
    print '\nlats in bbox :', grib_lats[grib_indexes].max(), grib_lats[grib_indexes].min()

# NDFD lons are 0 to 360, must adjust them to 0 to -180 used by NWS & ACIS
if verbose:
    print '  NDFD grib lats (0 to 90) :', grib_lats.min(), grib_lats.max()
    print ' NDFD grib lons (0 to 360) :', grib_lons.min(), grib_lons.max()
    print 'NDFD grib lons (0 to -180) :', neg_lons.min(), neg_lons.max()

# find nearest NDFD grid node to each ACIS grid node in region
print '\nmapping NDFD grid nodes to corresponding ACIS grid nodes'
COORD_MAPPING_START = datetime.datetime.now()

i_coords = acis_lats.shape[0]
j_coords = acis_lats.shape[1]
min_y = grib_indexes[0].min()
max_y = grib_indexes[0].max()
min_x = grib_indexes[1].min()
max_x = grib_indexes[1].max()
if extreme_debug:
    print 'min_y,max_y,min_x,max_x :', min_y,max_y,min_x,max_x
    print '  neg_lons[min_y,min_x] :', neg_lons[min_y,min_x]
    print '  neg_lons[max_y,max_x] :', neg_lons[max_y,max_x]
    print ' grib_lats[min_y,min_x] :', grib_lats[min_y,min_x]
    print ' grib_lats[max_y,max_x] :', grib_lats[max_y,max_x]

neg_lons = neg_lons[min_y:max_y,min_x:max_x]
if debug:
    print 'lat/lon grid size :', neg_lons.shape
    print 'min neg_lons :', neg_lons.min()
    print 'max neg_lons :', neg_lons.max()

grib_lats = grib_lats[min_y:max_y,min_x:max_x]
if debug:
    print 'min grib_lats :', grib_lats.min()
    print 'max grib_lats :', grib_lats.max()

for i in range(i_coords):
    for j in range(j_coords):
        acis_lon = acis_lons[i,j]
        acis_lat = acis_lats[i,j]
        lon_diffs = neg_lons - acis_lon
        lat_diffs = grib_lats - acis_lat
        diffs = N.sqrt( lon_diffs **2 + lat_diffs **2 )
        # track distance between ACIS node and nearest NDFD node
        min_diff = diffs.min()
        distance[i,j] = min_diff
        # indexes into grib array for this region
        nodes = N.where(diffs == min_diff)
        x_indexes[i,j] = x = nodes[1][0]
        y_indexes[i,j] = y = nodes[0][0]
        # track corresponding grib coords in case they are needed
        ndfd_lats[i,j] = grib_lats[y, x]
        ndfd_lons[i,j] = neg_lons[y, x]

        if extreme_debug:
            print '\n\nmin_diff :', min_diff
            print 'nodes :', nodes
            print 'x_indexes :', x_indexes[i,j]
            print 'y_indexes :', y_indexes[i,j]
            print ' distance :', distance[i,j]
            print ' ndfd_lat :', ndfd_lats[i,j]
            print ' acis_lat :', acis_lats[i,j]
            print ' ndfd_lon :', ndfd_lons[i,j]
            print ' acis_lon :', acis_lons[i,j]
            if j > 5: exit()

elapsed_time = elapsedTime(COORD_MAPPING_START, True)
fmt = 'finished mapping NDFD grid nodes to ACIS grid in %s' 
print fmt % elapsed_time

print '\nbuilding static file for %s region' % region
static_reader.open()
elevation = static_reader.get2DSlice('elev', bbox[0], bbox[2], bbox[1], bbox[3])
static_reader.close()
#filepath = static_factory.staticGridFilepath('ndfd', region)
#print "building", filepath

builder = ndfd_static_factory.ndfdStaticFileBuilder(region, dataset_shape)

with warnings.catch_warnings():
    builder.build(False, False, None, None, bbox=bbox, debug=debug)
    
    print '\nBuilding Acis datasets'
    data = {'lat':acis_lats, 'lon':acis_lons, 'elev':elevation, 'land_mask':None, 'interp_mask':None }
    builder.buildAcisDatasets(data, debug=debug)

    print '\nBuilding NDFD group datasets datasets'
    data = {'lat':ndfd_lats, 'lon':ndfd_lons, 'ndfd_dist':distance, 'ndfd_xidx':x_indexes, 'ndfd_yidx':y_indexes }
    builder.buildNdfdDatasets(data, debug=debug)

builder.close()

#builder.open()
#builder.buildNdfdDatasets(lons, lats, distance, x_indexes, y_indexes)
#builder.close()

# static file needs the following for acis :
#        lat, lon, elev, cus_interp_mask, cus_mask (all come from Acis5k conus static file)
#        lat, lon have attrs : view ('lat, lon') acis_grid = 3, units ('degrees')
#                              min/max (float), missing (float, N.nan)
#                              node_search_radius (float), node_spacing (float)
#                              source ('ACIS-HiRes'), source_detail ('ACIS-Hires grid 3')
#        elev has atrs : min, max, units ('m' for meters)
# plus a group for "ndfd" with the following
#        distance, lat, lon, x_indexes, y_indexes
#        distance, lat, lon attrs : min/max (float), region (full string)
#        distance also has : avg (float)
#        x_indexes, y_indexes attrs : offset (int = 0), min/max (int)
#        

elapsed_time = elapsedTime(START_TIME, True)
fmt = 'completed creation of NDFD static file for %s in %s' 
print fmt % (region, elapsed_time)
print 'filepath :', builder.filepath
