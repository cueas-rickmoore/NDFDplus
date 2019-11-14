#! /Volumes/Transport/venv2/ndfd/bin/python

import os
import datetime
UPDATE_START_TIME = datetime.datetime.now()

from atmosci.utils.timeutils import elapsedTime

from atmosci.seasonal.factory import NDFDProjectFactory

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()

parser.add_option('-f', action='store', dest='filetypes', default='maxt,mint')
parser.add_option('-p', action='store', dest='periods',
                        default='001-003,004-007')
parser.add_option('-r', action='store', dest='region', default='conus')
parser.add_option('-v', action='store_true', dest='verbose', default=False)
parser.add_option('-z', action='store_true', dest='debug', default=False)

options, args = parser.parse_args()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

debug = options.debug

if ',' in options.filetypes:
    filetypes = options.filetypes.split(',')
else: filetypes = [options.filetypes,]

if ',' in options.periods:
    periods = options.periods.split(',')
else: periods = [options.periods,]

verbose = options.verbose or debug

latest_time = datetime.datetime.utcnow()
target_year = latest_time.year

factory = NDFDProjectFactory()
target_date, filepaths = \
    factory.downloadLatestForecast(filetypes=filetypes, periods=periods,
                                   region=options.region, verbose=verbose)

elapsed_time = elapsedTime(UPDATE_START_TIME, True)
fmt = 'completed download for %s in %s' 
print fmt % (target_date.isoformat(), elapsed_time)

transport_dirpath = '/Volumes/Transport/data/app_data'
if os.path.exists(transport_dirpath):
    ndfd_dirpath = os.path.split(filepaths[0])[0]
    dest_dirpath = os.path.join(transport_dirpath, 'shared/forecast/ndfd')
    command = '/usr/bin/rsync -cgloprtuD %s %s' % (ndfd_dirpath, dest_dirpath)
    print command
    os.system(command)

