#! /Volumes/Transport/venv2/ndfd/bin/python

import os, sys
import warnings

import datetime

from atmosci.seasonal.factory import SeasonalStaticFileFactory
from atmosci.tempexts.factory import TempextsProjectFactory

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()

parser.add_option('-d', action='store_true', dest='dev_env', default=False)
parser.add_option('-f', action='store', dest='forecast_days', default=None)
parser.add_option('-r', action='store', dest='region', default=None)
parser.add_option('-s', action='store', dest='source', default=None)
parser.add_option('-v', action='store_true', dest='verbose', default=False)
parser.add_option('-z', action='store_true', dest='debug', default=False)

options, args = parser.parse_args()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

debug = options.debug
dev_env = options.dev_env
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
if dev_env: temps_factory.useDevEnv()
project = temps_factory.projectConfig()

static_factory = \
    SeasonalStaticFileFactory(temps_factory.config, temps_factory.registry)

region = temps_factory.regionConfig(options.region)
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

print 'building file :', filepath
if verbose: print 'file time span :', start_date, end_date
builder = temps_factory.tempextsFileBuilder(target_year, source, region,
                        start_date=start_date, end_date=end_date,
                        bbox=region.data, debug=debug)
builder.build(True, True)
reader = static_factory.staticFileReader(source, region)
builder.initLonLatData(reader.lons, reader.lats)
reader.close()
builder.close()

