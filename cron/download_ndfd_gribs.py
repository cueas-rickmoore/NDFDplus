#! /Volumes/Transport/venv2/ndfd/bin/python

import os
import datetime
UPDATE_START_TIME = datetime.datetime.now()

from atmosci.utils.timeutils import elapsedTime

from atmosci.ndfd.factory import NdfdGribFileFactory

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()

parser.add_option('-f', action='store', dest='filetypes', default='maxt,mint')
parser.add_option('-p', action='store', dest='periods',
                        default='001-003,004-007')
parser.add_option('-r', action='store', dest='region', default='conus')

parser.add_option('-d', action='store_true', dest='dev_mode', default=False)
parser.add_option('-v', action='store_true', dest='verbose', default=False)
parser.add_option('-z', action='store_true', dest='debug', default=False)

options, args = parser.parse_args()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

debug = options.debug
dev_mode = options.dev_mode
region = options.region
verbose = options.verbose or debug

if ',' in options.filetypes:
    filetypes = options.filetypes.split(',')
else: filetypes = [options.filetypes,]

if ',' in options.periods:
    periods = options.periods.split(',')
else: periods = [options.periods,]

latest_time = datetime.datetime.utcnow()
target_year = latest_time.year

factory = NdfdGribFileFactory()
if dev_mode: factory.useDirpathsForMode('dev')

target_date, filepaths = \
    factory.downloadLatestForecast(filetypes, periods, region, verbose)

elapsed_time = elapsedTime(UPDATE_START_TIME, True)
fmt = 'completed download of %d files on %s in %s' 
print fmt % (len(filepaths), target_date.isoformat(), elapsed_time)

if not dev_mode:
    transport_dirpath = '/Volumes/Transport/data/app_data'
    if os.path.exists(transport_dirpath):
        ndfd_dirpath = os.path.split(filepaths[0])[0]
        dest_dirpath = os.path.join(transport_dirpath, 'shared/forecast/ndfd')
        command = '/usr/bin/rsync -cgloprtuD %s %s' % (ndfd_dirpath, dest_dirpath)
        print command
        os.system(command)

