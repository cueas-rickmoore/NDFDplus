#! /Volumes/Transport/venv2/ndfd/bin/python

import os, sys
import warnings

import datetime
UPDATE_START_TIME = datetime.datetime.now()
from dateutil.relativedelta import relativedelta
ONE_DAY = relativedelta(days=1)

import numpy as N
import pygrib

from atmosci.utils.timeutils import asDatetimeDate, asAcisQueryDate, elapsedTime
from atmosci.utils.units import convertUnits

from atmosci.seasonal.factory import NDFDProjectFactory

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

INPUT_ERROR = 'You must pass a start date (year, month, day)'
INPUT_ERROR += ' and either the end date (month, day) or a number of days'

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()

parser.add_option('-r', action='store', dest='region', default=None)
parser.add_option('-s', action='store', dest='source', default='acis')

parser.add_option('-d', action='store_true', dest='dev_mode', default=False)
parser.add_option('-v', action='store_true', dest='verbose', default=False)
parser.add_option('-z', action='store_true', dest='debug', default=False)

options, args = parser.parse_args()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

debug = options.debug
dev_mode = options.dev_mode
region_key = options.region
source_key = options.source
verbose = options.verbose or debug

var_name_map = {'maxt':'Maximum temperature', 'mint':'Minimum temperature'}

today = datetime.date.today()
num_args = len(args)
if num_args == 0:
    target_date = today
elif num_args == 1:
    target_date = datetime.date(today.year, today.month, int(args[0]))
elif num_args == 2:
    target_date = datetime.date(today.year, int(args[0]), int(args[1]))
elif num_args == 3:
    target_date = datetime.date(int(args[0]), int(args[1]), int(args[2]))

factory = NDFDProjectFactory()
if dev_mode: factory.useDirpathsForMode('dev')
ndfd = factory.getSourceConfig('ndfd')
region = factory.getRegionConfig(region_key)
source = factory.getSourceConfig(source_key)
print 'updating % source file with NDFD forecast' % source.tag

# need indexes from static file for source
reader = factory.getStaticFileReader(source, region)
source_shape = reader.getDatasetShape('ndfd.x_indexes')
ndfd_indexes = [ reader.getData('ndfd.y_indexes').flatten(),
                 reader.getData('ndfd.x_indexes').flatten() ]
reader.close()
del reader

reader = factory.getSourceFileReader(source, target_date.year, region, 'temps')
last_obs_date = \
    asDatetimeDate(reader.getDatasetAttribute('temps.mint', 'last_obs_date'))
last_obs_mint = reader.getDataForDate('temps.mint', last_obs_date)
temps_filepath = reader.filepath
reader.close()
del reader

print '    last obs date', last_obs_date
if last_obs_date > target_date:
    last_obs_date = target_date
    last_obs_date_str = asAcisQueryDate(last_obs_date)
    print '    last obs date was corrupted in', temps_filepath
    manager = \
        factory.getSourceFileManager(source, target_date.year, region, 'temps', mode='a')
    manager.setDatasetAttribute('temps.maxt', 'last_obs_date', last_obs_date_str)
    manager.setDatasetAttribute('temps.mint', 'last_obs_date', last_obs_date_str)
    manager.close()
    print '    last obs date changed to', last_obs_date
    del manager

# create a template for the NDFD grib file path
filepath_template = \
    factory.forecastGribFilepath(ndfd, target_date, '%s', '%s')
# in case the daily download hasn't occurred yet - go back one day
if not os.path.exists(filepath_template % ('001-003', 'mint')):
    date = target_date - ONE_DAY
    filepath_template = factory.forecastGribFilepath(ndfd, date, '%s', '%s')


# filter annoying numpy warnings
warnings.filterwarnings('ignore',"All-NaN axis encountered")
warnings.filterwarnings('ignore',"All-NaN slice encountered")
warnings.filterwarnings('ignore',"invalid value encountered in greater")
warnings.filterwarnings('ignore',"invalid value encountered in less")
warnings.filterwarnings('ignore',"Mean of empty slice")
# MUST ALSO TURN OFF WARNING FILTERS AT END OF SCRIPT !!!!!

temps = { }
for temp_var in ('mint','maxt'):
    if temp_var == 'mint': daily = [(last_obs_date, last_obs_mint), ]
    else: daily = [ ]
    print '\nupdating forecast for', temp_var 

    for time_span in ('001-003','004-007'):
        grib_filepath = filepath_template % (time_span, temp_var)
        print '\nreading :', grib_filepath
        gribs = pygrib.open(grib_filepath)
        grib = gribs.select(name=var_name_map[temp_var])
        for message_num in range(len(grib)):
            message = grib[message_num]
            if debug: print message
            analysis_date = message.analDate
            print '    "analDate" =', analysis_date
            fcast_time = message.forecastTime
            if verbose: print '        "forecast time" =', fcast_time
            if fcast_time > 158: # forecast time is MINUTES
                fcast_time = analysis_date + relativedelta(minutes=fcast_time)
            else: # forecast time is hours
                fcast_time = analysis_date + relativedelta(hours=fcast_time)
            if verbose: print '        forecast datetime =', fcast_time
            fcast_date = fcast_time.date()
            
            if fcast_date > last_obs_date:
                data = message.values[ndfd_indexes].data
                data = data.reshape(source_shape)
                data[N.where(data == 9999)] = N.nan
                data = convertUnits(data, 'K', 'F')
                daily.append((fcast_date, data))
            else: print '        ignoring fcast for', fcast_date
        gribs.close()

        if len(daily) == 0:
            print 'NO TEMPERATURE DATAIN NDFD FORECAST FILE'
            sys.exit(99)

    temps[temp_var] = tuple(sorted(daily, key=lambda x: x[0]))
    if verbose: print temp_var, [item[0] for item in temps[temp_var]]

#target_year = fcast_date.year
target_year = target_date.year
manager = \
factory.getSourceFileManager(source, target_year, region, 'temps', mode='r')
manager.close()
print '\nsaving forecast to', manager.filepath

max_temps = temps['maxt']
min_temps = temps['mint']

print '\n\nmint, maxt date pairs :'
for indx in range(len(min_temps)):
    mint_date, mint = min_temps[indx]
    if mint_date >= last_obs_date:
        maxt_date, maxt = max_temps[indx]
        print '    ', mint_date, ',', maxt_date
        manager.open('a')
        manager.updateTempGroup(mint_date, mint, maxt, ndfd.tag, forecast=True)
        manager.close()

# turn annoying numpy warnings back on
warnings.resetwarnings()

# update forecast time span
fcast_start = min_temps[0][0]
fcast_end = min_temps[-1][0]
manager.open('a')
manager.setForecastDates('temps.maxt', fcast_start, fcast_end)
manager.setForecastDates('temps.mint', fcast_start, fcast_end)
manager.setForecastDates('temps.provenance', fcast_start, fcast_end)
manager.close()
del manager

elapsed_time = elapsedTime(UPDATE_START_TIME, True)
msg = '\ncompleted NDFD forecast update for %s thru %s in %s'
print msg % (fcast_start.strftime('%m-%d'), fcast_end.strftime('%m-%d, %Y'),
             elapsed_time)

