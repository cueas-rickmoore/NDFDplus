#! /Volumes/Transport/venv2/ndfd/bin/python

import os, sys
import datetime
import warnings

from atmosci.utils.timeutils import asDatetimeDate

from atmosci.tempexts.factory import TempextsProjectFactory

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()

parser.add_option('--da', action='store', type='int', dest='days_ago',
                  help='number of days to refresh prior to entered date',
                  default=None)
parser.add_option('--nd', action='store', type='int', dest='num_days',
                  help='number of days to refresh after entered date',
                  default=None)

parser.add_option('-r', action='store', dest='region', default=None)
parser.add_option('-s', action='store', dest='source', default=None)
parser.add_option('-v', action='store_true', dest='verbose', default=False)
parser.add_option('-z', action='store_true', dest='debug', default=False)

options, args = parser.parse_args()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

debug = options.debug
verbose = options.verbose or debug
print '\nrefresh_source_temp_grids.py', args

days_ago = options.days_ago
num_days = options.num_days

factory = TempextsProjectFactory()
project = factory.projectConfig()
region = factory.regionConfig(options.region)
source = factory.sourceConfig(options.source)

# figure out what dates to refresh
num_date_args = len(args)
if num_date_args == 0: # date span based on current day
    target_date = datetime.date.today()
    if days_ago: # refresh number of days ago ending yesterday
        end_date = target_date # - datetime.timedelta(days=1)
        start_date = target_date - datetime.timedelta(days=days_ago)
    elif num_days: # refresh num_days beginning today
        start_date = target_date
        end_date = target_date + datetime.timedelta(days=num_days)
    else: # just refresh the current day
        start_date = target_date
        end_date = None
elif num_date_args in (3,4,5):
    year = int(args[0])
    month = int(args[1])
    target_date = datetime.date(year,month,int(args[2]))
    if num_date_args == 4: # given start date and end day
        start_date = target_date
        end_date = datetime.date(year,month,int(args[3]))
    elif num_date_args == 5: # given start date and end month,day
        start_date = target_date
        end_date = datetime.date(year,int(args[3]),int(args[4]))
    else:
        if days_ago: # refresh number of days ago ending target date
            end_date = target_date
            start_date = end_date - datetime.timedelta(days=days_ago)
        elif num_days : # refresh number of days beginning on target date
            start_date = target_date
            end_date = start_date + datetime.timedelta(days=num_days)
        else: # just refresh a single day
            start_date = target_date
            end_date = None
else:
    print sys.argv
    errmsg = 'Invalid number of date arguments (%d).' % num_date_args
    raise ValueError, errmsg

# get get temperature data file manger
manager = factory.tempextsFileManager(start_date.year, source, region, 'r')
print 'refreshing temperature extremes in :\n', manager.filepath
# be sure to request data for the same grid that is already in the file
acis_grid = manager.datasetAttribute('temps.maxt', 'acis_grid')
# REFRESH MUST NEVER CHANGE ORIGINAL DATES IN maxt/mint DATASETS !!
last_obs_date = manager.datasetAttribute('temps.maxt', 'last_obs_date')
last_valid_date = manager.datasetAttribute('temps.maxt', 'last_valid_date')

if end_date is not None:
    # refresh start date cannot be earlier than first day in datasets
    # this is only relevant at the beginning of a new year
    data_limit = manager.dateAttribute('temps.maxt', 'start_date')
    start_date = max (start_date, data_limit)
    # refresh end date cannot be later the last day in the datasets
    # this is only relevant at the end of the year
    data_limit = manager.dateAttribute('temps.maxt', 'end_date')
    if end_date > data_limit: end_date = data_limit
    else:
        # refresh end date cannot be later than the last observation
        # otherwise, you risk stomping on the forecast
        end_date = min (end_date, asDatetimeDate(last_obs_date))

    # recalculate actual number of days to be refreshed 
    num_days = (end_date - start_date).days + 1

    msg = '    refreshing data for %d days : %s thru %s'
    print msg % (num_days, str(start_date), str(end_date))
else:
    if start_date < datetime.date(target_date.year, 1, 1):
        errmsg = "%s is an invalid date for %d"
        raise ValueError, errmsg % (str(start_date),target_date.year)
    print '    refreshing data for', str(start_date)

manager.close()

# filter annoying numpy warnings
warnings.filterwarnings('ignore',"All-NaN axis encountered")
warnings.filterwarnings('ignore',"All-NaN slice encountered")
warnings.filterwarnings('ignore',"invalid value encountered in greater")
warnings.filterwarnings('ignore',"invalid value encountered in less")
warnings.filterwarnings('ignore',"Mean of empty slice")
# MUST ALSO TURN OFF WARNING FILTERS AT END OF SCRIPT !!!!!

# download current ACIS mint,maxt for time span
data = factory.getAcisGridData(int(acis_grid), 'mint,maxt', start_date,
                               end_date, False, bbox=manager.data_bbox, 
                               debug=debug)
if debug: print 'temp data\n', data, '\n'

manager.open('a')
manager.updateTempGroup(start_date, data['mint'], data['maxt'], source.tag)
manager.close()
# REFRESH MUST NEVER CHANGE ORIGINAL last_obs_date or last_valid_date !!
manager.open('a')
manager.setDatasetAttribute('temps.maxt', 'last_obs_date', last_obs_date)
manager.setDatasetAttribute('temps.mint', 'last_obs_date', last_obs_date)
manager.setDatasetAttribute('temps.provenance', 'last_obs_date', last_obs_date)
manager.close()
manager.open('a')
manager.setDatasetAttribute('temps.maxt', 'last_valid_date', last_valid_date)
manager.setDatasetAttribute('temps.mint', 'last_valid_date', last_valid_date)
manager.setDatasetAttribute('temps.provenance', 'last_valid_date',
                            last_valid_date)
manager.close()

# turn annoying numpy warnings back on
warnings.resetwarnings()

