#! /Volumes/Transport/venv2/ndfd/bin/python

import os, sys
import warnings

import datetime

from atmosci.tempexts.factory import TempextsProjectFactory

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()

parser.add_option('-f', action='store_true', dest='update_forecast',
                  default=False)

parser.add_option('-r', action='store', dest='region', default=None)
parser.add_option('-s', action='store', dest='source', default=None)
parser.add_option('-v', action='store_true', dest='verbose', default=False)
parser.add_option('-z', action='store_true', dest='debug', default=False)

options, args = parser.parse_args()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

print '\ndownload_source_temp_grids.py', args

debug = options.debug
update_forecast = options.update_forecast
verbose = options.verbose or debug

factory = TempextsProjectFactory()
project = factory.projectConfig()

region = factory.regionConfig(options.region)
source = factory.sourceConfig(options.source)

end_date = None
num_date_args = len(args)
if num_date_args == 0:
    start_date = datetime.date.today()
elif num_date_args == 3:
    year = int(args[0])
    month = int(args[1])
    day = int(args[2])
    start_date = datetime.date(year,month,day)
elif num_date_args in (4,5,6):
    year = int(args[0])
    month = int(args[1])
    start_date = datetime.date(year,month,int(args[2]))
    if num_date_args == 4:
        end_date = datetime.date(year,month,int(args[3]))
    if num_date_args == 5:
        end_date = datetime.date(year, int(args[3]),int(args[4]))
    elif num_date_args == 6:
        end_date = datetime.date(int(args[3]),int(args[4]),int(args[5]))
else:
    print sys.argv
    errmsg = 'Invalid number of date arguments (%d).' % num_date_args
    raise ValueError, errmsg

# get a temperature data file manger
target_year = factory.targetYearFromDate(start_date)
filepath = factory.tempextsFilepath(target_year, source, region)
if debug:
    print 'temp filepath', os.path.exists(os.path.normpath(filepath)), filepath
if not os.path.exists(os.path.normpath(filepath)):
    manager = factory.tempextsFileBuilder(target_year, source, region)
else: manager = factory.tempextsFileManager(target_year, source, region, 'r')

acis_grid = manager.datasetAttribute('temps.maxt', 'acis_grid')
manager.close()

# get the last possible date that data might be available from this source
latest_available_date = factory.latestAvailableDate(source)
if end_date is None or end_date > latest_available_date:
    end_date = latest_available_date

if end_date == start_date:
    msg = 'downloding %s temps for %s'
    print msg % (source.tag, start_date.strftime('%B %d, %Y'))  
    end_date = None
else:
    msg = 'downloding %s temps for %s thru %s'
    print msg % (source.tag, start_date.strftime('%B %d'),  
                 end_date.strftime('%B %d, %Y'))

if debug:
    print 'temp manager', manager
    print 'temp manager file', manager.filepath


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
if debug: print 'temp data\n', data

print 'updating "temps" group'
manager.open('a')
manager.updateTempGroup(start_date, data['mint'], data['maxt'], source.tag)
manager.close()

# turn annoying numpy warnings back on
warnings.resetwarnings()

