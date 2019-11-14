#!/usr/bin/env python

import os, sys
import warnings

import datetime

from atmosci.seasonal.factory import SeasonalStaticFileFactory
from atmosci.tempexts.factory import TempextsProjectFactory

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()

parser.add_option('-d', action='store_true', dest='dev_mode', default=False)
parser.add_option('-f', action='store', dest='forecast_days', default=None)
parser.add_option('-r', action='store', dest='region', default=None)
parser.add_option('-s', action='store', dest='source', default='acis')
parser.add_option('-v', action='store_true', dest='verbose', default=False)
parser.add_option('-z', action='store_true', dest='debug', default=False)

options, args = parser.parse_args()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

debug = options.debug
dev_mode = options.dev_mode
region = options.region.upper()
source = options.source
verbose = options.verbose or debug
print '\ndownload_source_temp_grids.py', args

num_args = len(args)
if num_args == 0:
    today = datetime.date.today()
    if today.month == 12 and today.day >= 30:
        target_year = today.year + 1
    else: 
        target_year = today.year
else: target_year = int(args[0])

temps_factory = TempextsProjectFactory()
if dev_mode: temps_factory.useDirpathsForMode('dev')
project = temps_factory.projectConfig()

static_factory = SeasonalStaticFileFactory(temps_factory.config, temps_factory.registry)
if dev_mode: static_factory.useDirpathsForMode('dev')

region = temps_factory.regionConfig(region)
source = temps_factory.sourceConfig(options.source)

if options.forecast_days is not None:
    forecast_days = int(options.forecast_days)
else: forecast_days = project.get('forecast_days', 0)
if debug: print 'forecast_days :', forecast_days

start_date = datetime.date(target_year,1,1)
end_date = datetime.date(target_year,12,31)
if forecast_days > 0: end_date += datetime.timedelta(days=forecast_days)

# get a temperature data file manger
filepath = temps_factory.tempextsFilepath(target_year, source, region)
if debug: print 'filepath', os.path.exists(filepath), filepath
if os.path.exists(filepath): os.remove(filepath)

kwargs = { 'bbox':region.data, 'debug':debug }

print 'building file :', filepath
if verbose: print 'file time span :', start_date, end_date
builder = temps_factory.tempextsFileBuilder(target_year, source, region, start_date, end_date, **kwargs)
#builder.initFileAttributes(start_date=start_date, end_date=end_date, bbox=region.data)

reader = static_factory.staticFileReader(source, region)
builder.open('a')
builder.initLonLatData(reader.lons, reader.lats)
builder.close()
reader.close()

builder.open('a')
builder.buildGroup('temps', True, **kwargs)
builder.close()

