#! /Volumes/Transport/venv2/ndfd/bin/python

import os, sys
import warnings

import datetime
UPDATE_START_TIME = datetime.datetime.now()
from dateutil.relativedelta import relativedelta
ONE_DAY = relativedelta(days=1)

import numpy as N
import pygrib

from atmosci.utils.timeutils import asDatetimeDate, elapsedTime
from atmosci.utils.units import convertUnits

from atmosci.tempexts.factory import TempextsForecastFactory

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

INPUT_ERROR = 'You must pass a start date (year, month, day)'
INPUT_ERROR += ' and either the end date (month, day) or a number of days'

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()

parser.add_option('-v', action='store_true', dest='verbose', default=False)
parser.add_option('-z', action='store_true', dest='debug', default=False)

options, args = parser.parse_args()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

debug = options.debug
verbose = options.verbose or debug

var_name_map = {'maxt':'Maximum temperature', 'mint':'Minimum temperature'}

source_key = args[0]
region_key = args[1]

if len(args) ==2:
    target_date = datetime.date.today()
else:
    target_date = datetime.date(int(args[2]), int(args[3]), int(args[4]))

factory = TempextsForecastFactory()
ndfd = factory.sourceConfig('ndfd')
region = factory.regionConfig(region_key)
source = factory.sourceConfig(source_key)
print 'updating % source file with NDFD forecast' % source.tag

# need indexes from static file for source
reader = factory.staticFileReader(source, region)
source_shape = reader.datasetShape('ndfd.x_indexes')
ndfd_indexes = [ reader.getData('ndfd.y_indexes').flatten(),
                 reader.getData('ndfd.x_indexes').flatten() ]
reader.close()
del reader

reader = factory.tempextsFileReader(target_date.year, source, region)
last_obs_date = reader.dateAttribute('temps.mint', 'last_obs_date')
print '    last obs date', last_obs_date
del reader

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
for temp_var in ('maxt','mint'):
    daily = [ ]
    print '\nupdating forecast for', temp_var 

    for time_span in ('001-003','004-007'):
        grib_filepath = filepath_template % (time_span, temp_var)
        print '\nreading :', grib_filepath
        gribs = pygrib.open(grib_filepath)
        grib = gribs.select(name=var_name_map[temp_var])
        for message_num in range(len(grib)):
            message = grib[message_num]
            analysis_date = message.analDate
            print '    "analDate" =', analysis_date
            fcast_time = message.forecastTime
            if verbose: print '        "forecastTime" =', fcast_time
            if fcast_time > 158: # forecastTime is MINUTES
                fcast_time = analysis_date + relativedelta(minutes=fcast_time)
            else: # forecastTime is hoors
                fcast_time = analysis_date + relativedelta(hours=fcast_time)
            if verbose: print '        forecast datetime =', fcast_time
            fcast_date = fcast_time.date()

            if fcast_date > last_obs_date:
                data = message.values[ndfd_indexes].data
                data = data.reshape(source_shape)
                data[N.where(data == 9999)] = N.nan
                data = convertUnits(data, 'K', 'F')
                print '        forecast date =', fcast_date
                daily.append((fcast_date, data))
            print ' '
        gribs.close()

    temps[temp_var] = tuple(sorted(daily, key=lambda x: x[0]))

#print '\n\n', temps['mint']
#print '\n\n', temps['maxt']

target_year = fcast_date.year
manager = \
factory.tempextsFileManager(source, target_year, region, 'temps', mode='a')
print '\nsaving forecast to', manager.filepath

max_temps = temps['maxt']
min_temps = temps['mint']

for indx, data in enumerate(min_temps):
    mint_date, mint = data
    if mint_date > target_date:
        maxt_date, maxt = max_temps[indx]
        manager.open('a')
        manager.updateTempGroup(mint_date, mint, maxt, ndfd.tag, forecast=True)
        manager.close()

# turn annoying numpy warnings back on
warnings.resetwarnings()

# update forecast time span
first_date = min_temps[0][0]
last_date = min_temps[-1][0]
manager.open('a')
manager.setForecastDates('temps.maxt', first_date, last_date)
manager.setForecastDates('temps.mint', first_date, last_date)
manager.setForecastDates('temps.provenance', first_date, last_date)
manager.close()
del manager

elapsed_time = elapsedTime(UPDATE_START_TIME, True)
msg = '\ncompleted NDFD forecast update for %s thru %s in %s'
print msg % (first_date.strftime('%m-%d'), last_date.strftime('%m-%d, %Y'),
             elapsed_time)

