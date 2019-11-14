#! /Volumes/Transport/venv2/ndfd/bin/python

import os, sys
import datetime
import time
from dateutil.relativedelta import relativedelta

from cuas.utils.logging import logException
from cuas.utils.report import Reporter
from cuas.utils.string import tupleFromString

from cuas.stations.manager import StationDataFileManager
from cuas.stations.builder import StationDataFileBuilder
from cuas.stations.scripts.factory import StationDataManagerFactory

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

#APP = os.path.splitext(os.path.split(__file__)[1])[0].upper().replace('_',' ')
APP = os.path.split(sys.argv[0])[1] + ' ' + ' '.join(sys.argv[1:])
#PID = os.getpid()
PID = 'PID %d' % os.getpid()

from cuas.config import STATIONS, REGION

DEFAULT_ELEMENTS = ( {'name':'pcpn', 'add':['f','t']},
                     {'name':'maxt', 'add':['f','t']},
                     {'name':'mint', 'add':['f','t']},
                   )
DEFAULT_METADATA = 'uid,name,ll,elev'

ONE_DAY = relativedelta(days=1)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()
parser.add_option('-e', action='store', type='string', dest='elements',
                  default=DEFAULT_ELEMENTS)
parser.add_option('-l', action='store', type='string', dest='log_filepath',
                  default=None,
                  help='path to alternate file to be used for logging')
parser.add_option('-m', action='store', type='string', dest='metadata',
                  default=DEFAULT_METADATA)
parser.add_option('-ma', action='store', type='string', dest='max_attempts',
                  default=5)
parser.add_option('-r', action='store', type='string', dest='region',
                  default='EOR', help="Region.")
parser.add_option('--sf', action='store', type='string', default=None,
                  dest='station_filepath')
parser.add_option('--st', action='store', type='int', dest='sleep_time',
                  default=120)
parser.add_option('-u', action='store', type='string', dest='base_url',
                  default=STATIONS.download_url)
parser.add_option('--wt', action='store', type='int', dest='wait_time',
                  default=10)
parser.add_option('-x', action='store_true', dest='replace_existing',
                  default=False)
parser.add_option('-z', action='store_true', dest='debug', default=False)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

reporter = Reporter(PID)

try:
    start_perf = datetime.datetime.now()

    options, args = parser.parse_args()

    if options.log_filepath is None:
        reporter = Reporter(PID, os.path.join(STATIONS.working_dir,
                                              'cron-jobs.log'))
    else:
        if options.log_filepath == 'no':
            reporter = Reporter(PID, None)
        else:
            reporter = Reporter(PID, os.path.normpath(options.log_filepath))
    if reporter.filepath is not None:
        reporter.logEvent(APP)

    if len(args) > 0:
        year = int(args[0])
        month = int(args[1])
        day = int(args[2])
        date = datetime.date(year,month,day)
    else:
        date = datetime.date.today()
        if options.days_ago > 0:
            date -= relativedelta(days=options.days_ago)
        elif options.weeks_ago > 0:
            date -= relativedelta(weeks=options.weeks_ago)
        elif options.months_ago > 0:
            date -= relativedelta(months=options.months_ago)
        year = date.year
        month = date.month
        day = date.day

    debug = options.debug
    if type(options.elements) in (dict,tuple,list):
        elements = options.elements
    else:
        elements = tupleFromString(options.elements)
    max_attempts = options.max_attempts
    metadata = tupleFromString(options.metadata)
    replace_existing = options.replace_existing
    wait_time = options.wait_time

    factory = StationDataManagerFactory(date, options)
    station_filepath = factory.getFilepath('stations')
    working_dir = factory.getDirectory('working')

    region_name = factory.region_name
    states = factory.region.states
    file_attributes =  { 'bbox':factory.region_bbox,
                         'data_bbox':factory.data_bbox
                       }

    # discover days that need running
    dates = [ ]
    num_days = 0

    while num_days < options.max_days:
        factory = StationDataManagerFactory(date, options)
        station_filepath = factory.getFilepath('region')
        if os.path.exists(station_filepath):
            if replace_existing:
                os.remove(station_filepath)
                dates.append((date,station_filepath))
        else:
            dates.append((date,station_filepath))
        date -= ONE_DAY
        num_days+=1
except:
    logException(APP, reporter, 'Application initialization')
    os._exit(1)

if len(dates) < 1: os._exit(0)
dates.sort()

# download station data for each day
failed_dates = [ ]
for date, station_filepath in dates:
    start_day = datetime.datetime.now()

    date_str = '%02d-%02d-%d' % (date.month,date.day,date.year)
    # give ACIS a chance to recover between dates
    if num_days > 0: time.sleep(wait_time)
    
    msg = 'Downloading station data for %s' 
    reporter.logInfo(msg % date.strftime('%Y-%m-%d'))

    try:
        builder = StationDataFileBuilder(StationDataFileManager,
                                         station_filepath, elements, metadata,
                                         options.base_url, file_attributes,
                                         options.sleep_time, reporter)
        valid, total = builder((date.year,date.month,date.day), states,
                                max_attempts, debug=debug, performance=True)
    except Exception:
        errmsg = 'Download of station data for %s failed with an exception'
        logException(APP, reporter, errmsg % date_str)
        failed_dates.append(date_str)
    else:
        msg = "%d stations saved to file %s" % (valid, station_filepath)
        reporter.logInfo(msg)
        msg = 'Time to build station data file ='
        reporter.logPerformance(start_day, msg)

if failed_dates:
    errmsg = 'Station data download failed for %d dates : %s ' %\
             (len(failed_dates), ', '.join(failed_dates))
    reporter.logError(errmsg)
    if reporter.filepath is not None:
        reporter.reportEvent(errmsg)
        reporter.reportEvent('For details, see log file : %s' % reporter.filepath)
    reporter.reportEvent('process ended with unresolved errors')
    if len(dates) > len(failed_dates):
        os._exit(90)
    os._exit(99)
else:
    reporter.logEvent('process completed successfully')
if reporter.filepath is not None:
    reporter.flush()

