#! /usr/bin/env python

import os, sys
import warnings
import datetime

import numpy as N

from atmosci.tempexts.factory import TempextsProjectFactory

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()

parser.add_option('-d', action='store_true', dest='dev_mode', default=False)
parser.add_option('-r', action='store', dest='region', default=None)
parser.add_option('-s', action='store', dest='source', default='acis')
parser.add_option('-v', action='store_true', dest='verbose', default=False)
parser.add_option('-z', action='store_true', dest='debug', default=False)

options, args = parser.parse_args()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

debug = options.debug
dev_mode = options.dev_mode
verbose = options.verbose or debug

if debug: print '\n%s\n' % sys.argv[0], args

factory = TempextsProjectFactory()
if dev_mode: factory.useDirpathsForMode('dev')

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
print '\nRefreshing provenance\n', filepath
print '    from %s thru %s\n' % (str(start_date), str(end_date))

manager = factory.tempextsFileManager(target_year, source, region, 'r')
manager.debug = debug
manager.verbose = verbose
manager.close()

# filter annoying numpy warnings
warnings.filterwarnings('ignore',"All-NaN axis encountered")
warnings.filterwarnings('ignore',"All-NaN slice encountered")
warnings.filterwarnings('ignore',"invalid value encountered in greater")
warnings.filterwarnings('ignore',"invalid value encountered in less")
warnings.filterwarnings('ignore',"Mean of empty slice")
# MUST ALSO TURN OFF WARNING FILTERS AT END OF SCRIPT !!!!!

manager.open('a')
manager.refreshTempProvenance(start_date, end_date, options.source)
manager.close()

# turn annoying numpy warnings back on
warnings.resetwarnings()

