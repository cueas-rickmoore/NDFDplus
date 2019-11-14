#! /Volumes/Transport/venv2/ndfd/bin/python

import os, sys
from datetime import datetime

import numpy as N

from atmosci.ag.linvill import linvill_interpolators
from atmosci.ag.linvill._archive.art import linvill as arts_linvill

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()

# station search criteria
parser.add_option('--lat', action='store', type='float', dest='lat',
                  default=42.1)
parser.add_option('--maxt', action='store', type='float', dest='maxt',
                  default=55)
parser.add_option('--mint', action='store', type='float', dest='mint',
                  default=35)

parser.add_option('-a', action='store_true', dest='test_array', default=False)
parser.add_option('-g', action='store_true', dest='test_grid', default=False)
parser.add_option('-p', action='store_true', dest='test_point', default=False)

options, args = parser.parse_args()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

separator = '*' * 72

if len(args) == 3:
    date = (int(args[0]), int(args[1]), int(args[2]))
else: date = datetime.now()

lat = float(options.lat)
maxt = float(options.maxt)
mint = float(options.mint)

if options.test_point:
    print '\npoint interpolator'
    print separator
    interpolate = linvill_interpolators['point']
    hourly = interpolate(date, lat, mint, maxt, 'F', debug=True)
    print '\nhourly temps', hourly

    print "\nArt's interpolation results"
    print separator
    art = arts_linvill(date, lat, mint, maxt, debug=True)
    print '\nhourly temps'
    print hourly
    print '\ndifferences'
    print hourly - art

if options.test_array:
    lat_array = N.array([lat for i in range(10)], dtype=float)
    maxt_array = N.array([maxt for i in range(10)], dtype=float)
    mint_array = N.array([mint for i in range(10)], dtype=float)

    print '\narray interpolator'
    print lat_array
    print separator
    interpolate = linvill_interpolators['array']
    hourly = interpolate(date, lat_array, mint_array, maxt_array, 'F',
                         debug=True)
    print '\nhourly for first item'
    print hourly[:,0]
    print '\nhourly for last item'
    print hourly[:, -1]

    print "\nArt's interpolation results"
    print separator
    art = arts_linvill(date, lat, mint, maxt, debug=True)
    print '\nhourly temps'
    print art
    print '\ndifferences for first item'
    print N.subtract(hourly[:,0], art)
    print '\ndifferences for last item'
    print N.subtract(hourly[:,-1], art)

if options.test_grid:
    row = [lat for i in range(4)]
    lat_grid = N.array( [row for i in range(4)], dtype=float )

    row = [maxt for i in range(4)]
    maxt_grid = N.array( [row for i in range(4)], dtype=float )

    row = [mint for i in range(4)]
    mint_grid = N.array( [row for i in range(4)], dtype=float )

    print '\ngrid interpolator'
    print separator
    interpolate = linvill_interpolators['grid']
    hourly = interpolate(date, lat_grid, mint_grid, maxt_grid, 'F', debug=True)
    print '\nhourly @ node[0,0]'
    print hourly[:,0,0]
    print '\nhourly @ node[-1,-1]'
    print hourly[:,-1,-1]

    print "\nArt's interpolation results"
    print separator
    art = arts_linvill(date, lat, mint, maxt, debug=True)
    print '\nhourly temps'
    print art
    print '\ndifferences @ node[0,0]'
    print N.subtract(hourly[:,0,0], art)
    print '\ndifferences @ node[-1,-1]'
    print N.subtract(hourly[:,-1,-1], art)

