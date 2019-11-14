#!/usr/bin/env python

import os, sys
import datetime
BUILD_START_TIME = datetime.datetime.now()

from dateutil.relativedelta import relativedelta
ONE_DAY = relativedelta(days=1)

import numpy as N

from atmosci.utils.timeutils import elapsedTime
from atmosci.seasonal.factory import AcisProjectFactory

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

PERFORMANCE_MSG = 'completed build for %d in %s'

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()

parser.add_option('-i', action='store', dest='increment', type='int',
                  default=10)
parser.add_option('-r', action='store', dest='region', default=None)
parser.add_option('-s', action='store', dest='source', default=None)
parser.add_option('-v', action='store_true', dest='verbose', default=False)
parser.add_option('-z', action='store_true', dest='debug', default=False)

options, args = parser.parse_args()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

INCREMENT = relativedelta(days=options.increment-1)

debug = options.debug
verbose = options.verbose

factory = AcisProjectFactory()
project = factory.getProjectConfig()
if verbose:
    print 'project :\n', project

region_key = options.region
if region_key is None: region_key = project.region
if len(region_key) == 2: region_key = region_key.upper()
region = factory.getRegionConfig(region_key)
bbox = region.data
if verbose:
    print '\nregion :\n', region

source_key = options.source
if source_key is None: source_key = project.source
source = factory.getSourceConfig(source_key)
if verbose:
    print '\nsource :\n', source
    print ' '
latest_available_date = factory.latestAvailableDate(source)
latest_available_time = \
    factory.latestAvailableTime(source, latest_available_date)

current_year = BUILD_START_TIME.year
if len(args) == 0:
    target_years = [current_year,]
elif len(args) == 1:
    arg = args[0]
    if arg.isdigit():
        target_years = [int(arg),]
    else:
        scope = list(project.scopes[arg])
        if scope[1] == 9999: scope[1] = current_year - 1
        target_years = [year for year in range(scope[0],scope[1]+1)]
elif len(args) == 2:
    target_years =  [year for year in range(int(args[0]),int(args[1])+1)]
else: target_years = [int(arg) for arg in args]

filetpye = factory.getFiletypeConfig('source')
groups_in_file = filetpye.groups

for target_year in target_years:
    year_start_time = datetime.datetime.now()

    start_date = datetime.date(target_year,1,1)
    if start_date > latest_available_date:
        print '%s data is not available for %d' % (source.tag, target_year)
        continue

    end_date = datetime.date(target_year,12,31)
    if end_date > latest_available_date:
        max_end_date = latest_available_date
        errmsg = '%s data is only available thru %s'
        print errmsg % (source.tag, latest_available_date.isoformat())
    else: max_end_date = end_date

    # create a build and initialize the file
    builder = factory.getSourceFileBuilder(source, target_year, region,
                                           'temps', bbox=bbox)
    print 'building', builder.filepath
    if verbose:
        print '\nbuilder source :\n', builder.source
        print '\nbuilder region :\n', builder.region

    # build the groups and their datasets
    for group_name in groups_in_file:
        if verbose: print '    building group : %s' % group_name
        builder.buildGroup(group_name, True)

    # get first month plus metadata
    end_date = start_date + INCREMENT
    if verbose: print 'downloading', start_date, 'thru', end_date
    data = factory.getAcisGridData(source_key, 'maxt,mint', start_date,
                   end_date, False, meta='ll', bbox=bbox, debug=debug)
    # create lat,lon grids
    if verbose: print '    creating lat/lon datasets'
    builder.initLonLatData(data['lon'], data['lat'])
    del data['lon']
    del data['lat']

    # insert temperature data for the next time sequence
    builder.open('a')
    builder.updateTempGroup(start_date, data['maxt'], data['mint'],
                            source.tag)
    builder.close()
    if verbose: print '    inserted daily temperatures'

    # retrieve data for the rest of the year
    while end_date < max_end_date:
        if verbose: print '\ndownloading', start_date, 'thru', end_date
        data = factory.getAcisGridData(source_key, 'maxt,mint', start_date,
                       end_date, False, bbox=bbox, debug=debug)
        builder.open('a')
        builder.updateTempGroup(start_date, data['maxt'], data['mint'],
                                source.tag)
        builder.close()
        if verbose: print '    inserted daily temperatures'

        # incrment to next sequence of dates
        start_date = end_date + ONE_DAY
        end_date = start_date + INCREMENT

    # days left over, update them
    if start_date < max_end_date:
        if verbose: print '\ndownloading', start_date, 'thru', max_end_date
        data = factory.getAcisGridData(source_key, 'maxt,mint', start_date,
                       max_end_date, False, bbox=bbox, debug=debug)
        builder.open('a')
        builder.updateTempGroup(start_date, data['maxt'], data['mint'],
                                source.tag)
        builder.close()
        if verbose: print '    inserted daily temperatures'
        del data

    elapsed_time = elapsedTime(year_start_time, True)
    print PERFORMANCE_MSG % (target_year, elapsed_time)

elapsed_time = elapsedTime(BUILD_START_TIME, True)
print 'completed build for %d years in %s' % (len(target_years), elapsed_time)

