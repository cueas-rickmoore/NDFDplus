#! /Volumes/Transport/venv2/ndfd/bin/python

import os
import datetime
UPDATE_START_TIME = datetime.datetime.now()

from atmosci.utils.timeutils import elapsedTime

from atmosci.seasonal.factory import NDFDProjectFactory

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()

parser.add_option('-n', action='store_true', dest='use_ndfd_cache',
                  default=False)
parser.add_option('-v', action='store_true', dest='verbose', default=False)
parser.add_option('-z', action='store_true', dest='debug', default=False)

options, args = parser.parse_args()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

debug = options.debug
use_ndfd_cache = options.use_ndfd_cache
verbose = options.verbose or debug

latest_time = datetime.datetime.utcnow()
target_year = latest_time.year

factory = NDFDProjectFactory()
if use_ndfd_cache:
    factory.setServerUrl(factory.ndfd_config.cache_server)

target_date, filepaths = \
    factory.downloadLatestForecast(filetypes=('maxt','mint'),
                                   periods=('001-003','004-007'),
                                   region='conus', verbose=True)

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

