#! /Volumes/Transport/venv2/ndfd/bin/python

import os, sys
import datetime
import numpy as N

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

HOURLY_URL = 'http://data.nrcc.rcc-acis.org/StnData'
PARAMS = '?params={"sid":"%(station)s icao",%(dates)s,"elems":"%(elems)s"}'

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()
parser.add_option('-a', action='store', type='string', dest='max_attempts',
                  default=5)
parser.add_option('-b', action='store', type='string', dest='bbox',
                  default='-125.0,24.0,-66.04166,49.95833')
parser.add_option('-g', action='store', type='int', dest='grid', default=1)
parser.add_option('-l', action='store', type='string', dest='log_filepath',
                  default=None,
                  help='path to alternate file to be used for logging')
parser.add_option('-m', action='store', type='string', dest='metadata',
                  default=None)
parser.add_option('-r', action='store', type='string', dest='region',
                  default='EOR', help="Region.")
parser.add_option('--st', action='store', type='int', dest='sleep_time',
                  default=120)
parser.add_option('-u', action='store', type='string', dest='base_url',
                  default=None)
parser.add_option('--wt', action='store', type='int', dest='wait_time',
                  default=10)
parser.add_option('-z', action='store_true', dest='debug', default=False)

options, args = parser.parse_args()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

debug = options.debug
max_attempts = options.max_attempts
wait_time = options.wait_time

args = { 'dates':'"sdate":"2017-07-13","edate":"2017-07-28"',
         'elems':'5,22,23,24'
       }

for station in ("KALB","KYNG","KCAR","KSLK","KEKN"):
    args['station'] = station
    print HOURLY_URL + PARAMS % args

