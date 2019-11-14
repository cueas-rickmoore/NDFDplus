#! /Volumes/Transport/venv2/ndfd/bin/python

import os, sys
import warnings

import datetime

from atmosci.tempexts.grid import TemperatureFileReader, TemperatureFileBuilder
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
print '\ncopy_acis_temp_grids.py', args

num_args = len(args)
if num_args == 0:
    today = datetime.date.today()
    if today.month == 12 and today.day >= 30:
        target_year = today.year + 1
    else: 
        target_year = today.year
else: target_year = int(args[0])

factory = TempextsProjectFactory()
if dev_env: factory.useDevEnv()
project = factory.projectConfig()

region = factory.regionConfig(options.region)
source = factory.sourceConfig(options.source)

if options.forecast_days is not None:
    forecast_days = int(options.forecast_days)
else: forecast_days = project.get('forecast_days', 0)
if debug: print 'forecast_days :', forecast_days

start_date = datetime.date(target_year,1,1)
end_date = datetime.date(target_year,12,31)
if forecast_days > 0: end_date += datetime.timedelta(days=forecast_days)

filepath = factory.tempextsFilepath(target_year, source, region)
print 'copying file :', filepath

dest_filepath = filepath.replace('.h5', '-new.h5')
if debug: print 'dest_filepath', os.path.exists(dest_filepath), dest_filepath
if os.path.exists(dest_filepath): os.remove(dest_filepath)

reader = TemperatureFileReader(filepath, factory.registry)

print 'creating file :', dest_filepath
if verbose: print 'new file time span :', start_date, end_date
builder = TemperatureFileBuilder(dest_filepath, factory.registry,
                         factory.config, target_year, source, region,
                         start_date=start_date, end_date=end_date,
                         debug=debug)
builder.open('a')
builder.setFileAttributes(**reader.fileAttributes())
builder.close()
builder.open('a')
builder.build(True, True)
builder.close()
builder.open('a')
builder.initLonLatData(reader.lons, reader.lats)
builder.close()
# copy exsting temperatures to new file
max_valid = reader.dateAttribute('temps.maxt', 'last_valid_date')
builder.open('a')
builder.updateTempGroup(start_date,
                        reader.dateSlice('temps.mint', start_date, max_valid),
                        reader.dateSlice('temps.maxt', start_date, max_valid),
                        source.tag)
builder.close()

# update dataset date attributes to match existing data
date_attrs = reader.dateAttributes('temps.maxt')
# update end_date on new file to match new end date
date_attrs['end_date'] = end_date
builder.open('a')
builder.setDateAttributes('temps.maxt', **date_attrs)
builder.setDateAttributes('temps.mint', **date_attrs)
builder.setDateAttributes('temps.provenance', **date_attrs)
builder.close()
reader.close()

