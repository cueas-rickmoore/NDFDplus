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

from atmosci.seasonal.factory import NDFDProjectFactory

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

INPUT_ERROR = 'You must pass a start date (year, month, day)'
INPUT_ERROR += ' and either the end date (month, day) or a number of days'

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def updateForecast(mint_temps, max_temps, fcast_start, target_year, source, region):
    # filter annoying numpy warnings
    warnings.filterwarnings('ignore',"All-NaN axis encountered")
    warnings.filterwarnings('ignore',"All-NaN slice encountered")
    warnings.filterwarnings('ignore',"invalid value encountered in greater")
    warnings.filterwarnings('ignore',"invalid value encountered in less")
    warnings.filterwarnings('ignore',"Mean of empty slice")
    # MUST ALSO TURN OFF WARNING FILTERS AT END OF FUNCTION !!!!!

    manager = factory.getSourceFileManager(source, target_year, region,
                                           'temps', mode='a')
    print '\nsaving forecast to', manager.filepath

    first_date = None # first date in forecast for target year
    last_date = None # last date in forecast for target year
    # pop first item from each of the forecast lists
    mint_date, mint = min_temps.pop()

    # pop items from the forecast lists until gone, or end of current year
    first_of_next_year = datetime.date(fcast_start.year+1, 1, 1)
    while mint_date < first_of_next_year:
        if mint_date > target_date: # skip forecasts prior to current date
            maxt_date, maxt = max_temps.pop()
            manager.open('a')
            manager.updateTempGroup(mint_date, mint, maxt, ndfd.tag, forecast=True)
            manager.close()
            if first_date is None: first_date = mint_date
            last_date = mint_date
        # get mint for the next day in forecast
        try:
            mint_date, mint = min_temps.pop()
        except IndexError: # all days from forecast have be added
            break

    # forecast has data for target_year
    if first_date is not None:
        # update forecast time span in current year's file
        manager.open('a')
        manager.setForecastDates('temps.maxt', first_date, last_date)
        manager.setForecastDates('temps.mint', first_date, last_date)
        manager.setForecastDates('temps.provenance', first_date, last_date)
        manager.close()
        del manager

    # turn annoying numpy warnings back on
    warnings.resetwarnings()

    # at least one day in the next year
    if len(maxt_temps) > 0:
        mint_temps.insert(0, [mint_date, mint])
    return mint_temps, max_temps

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
target_year = target_date.year

factory = NDFDProjectFactory()
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

fcast_start = None
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
            if verbose: print '    "forecastTime" =', fcast_time
            if fcast_time > 158: # forecastTime is MINUTES
                fcast_time = analysis_date + relativedelta(minutes=fcast_time)
            else: # forecastTime is hoors
                fcast_time = analysis_date + relativedelta(hours=fcast_time)
            if verbose: print '    forecast datetime =', fcast_time
            fcast_date = fcast_time.date()
            print '    forecast date =', fcast_date

            if fcast_date > last_obs_date:
                data = message.values[ndfd_indexes].data
                data = data.reshape(source_shape)
                data[N.where(data == 9999)] = N.nan
                data = convertUnits(data, 'K', 'F')
                daily.append((fcast_date, data))
                if fcast_start is None: fcast_start = fcast_date
            print ' '
        gribs.close()

    temps[temp_var] = daily

# turn annoying numpy warnings back on
warnings.resetwarnings()

# bad forecast file, no future dates
if fcast_start is None:
    print "Forecast contains no data beyond last obs date", last_obs_date
    os._exit(99)

# forecast at least partialy in the current year
if fcast_start.year == target_year:
    min_temps, max_temps = updateForecast(temps['mint'], temps['maxt'], fcast_start,
                                          target_year, source, region)
else:
    min_temps = temps['mint'] 
    max_temps = temps['maxt']

if len(mint_temps) > 0:
    manager = factory.getSourceFileManager(source, target_year, region,
                                           'temps', mode='a')
    if manager.datasetHasAttr('temps.maxt', 'fcast_start'):
        manager.deleteDatasetAttribute('temps.maxt', 'fcast_start')
    if manager.datasetHasAttr('temps.maxt', 'fcast_end'):
        manager.deleteDatasetAttribute('temps.maxt', 'fcast_end')
    if manager.datasetHasAttr('temps.mint', 'fcast_start'):
        manager.deleteDatasetAttribute('temps.mint', 'fcast_start')
    if manager.datasetHasAttr('temps.mint', 'fcast_end'):
        manager.deleteDatasetAttribute('temps.mint', 'fcast_end')
    if manager.datasetHasAttr('temps.provenance', 'fcast_start'):
        manager.deleteDatasetAttribute('temps.provenance', 'fcast_start')
    if manager.datasetHasAttr('temps.provenance', 'fcast_end'):
        manager.deleteDatasetAttribute('temps.provenance', 'fcast_end')
    manager.close()
    del manager

    fcast_start = min_temps[0][0]
    target_year = fcast_start.year
    min_temps, max_temps = \
        updateForecast(min_temps, max_temps, fcast_start, target_year, source, region)

elapsed_time = elapsedTime(UPDATE_START_TIME, True)
msg = '\ncompleted NDFD forecast update for %s thru %s in %s'
print msg % (first_date.strftime('%m-%d'), last_date.strftime('%m-%d, %Y'),
             elapsed_time)

