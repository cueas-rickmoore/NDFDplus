#! /Volumes/Transport/venv2/ndfd/bin/python

import os, sys
import warnings

import datetime

from atmosci.utils.timeutils import asDatetimeDate
from atmosci.tempexts.grid import TemperatureFileManager
from atmosci.tempexts.factory import TempextsProjectFactory

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()

parser.add_option('-d', action='store_true', dest='dev_env', default=False)
parser.add_option('-f', action='store_true', dest='forecast', default=False)
parser.add_option('-r', action='store', dest='region', default=None)
parser.add_option('-s', action='store', dest='source', default=None)
parser.add_option('-v', action='store_true', dest='verbose', default=False)
parser.add_option('-z', action='store_true', dest='debug', default=False)

options, args = parser.parse_args()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

debug = options.debug
dev_env = options.dev_env
forecast = options.forecast
verbose = options.verbose or debug
print '\ntest_acis_temps.py', args

factory = TempextsProjectFactory()
if dev_env: factory.useDevEnv()

arg_0 = args[0]
if arg_0.isdigit():
    target_year = int(args[0])
    region = factory.regionConfig(options.region)
    source = factory.sourceConfig(options.source)
    filepath = factory.tempextsFilepath(target_year, source, region)
else:
    filepath = os.path.abspath(arg_0)
print 'testing in file :', filepath

from_dates = input("Enter dates to copy : ")
if debug: print 'from_dates =', from_dates
if ',' in from_dates:
    start_date, end_date = from_dates.split(',')
    start_date = asDatetimeDate(start_date)
    end_date = asDatetimeDate(end_date)
else: start_date = end_date = asDatetimeDate(from_dates)

print 'Copying data from', start_date, 'to', end_date
manager = TemperatureFileManager(filepath, factory.registry, mode='r')
mint = manager.dateSlice('temps.mint', start_date, end_date)
maxt = manager.dateSlice('temps.maxt', start_date, end_date)
if forecast: source = 'NDFD'
else: source = manager.fileAttribute('source')
manager.close()

to_date = asDatetimeDate(input("Enter date to copy data to : "))
if debug: print 'to_date :', to_date
print 'Inserting data at', to_date

manager.open('a')
manager.updateTempGroup(to_date, mint, maxt, source, forecast=forecast)
manager.close()

