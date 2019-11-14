#! /Volumes/Transport/venv2/ndfd/bin/python

USAGE = """%prog [options] grid_id data_key filepath

Arguments: grid_file = numeric identifier used by grid component factory to 
                       access grid information.
           data_key = name the gridded dataset to be plotted.
           filepath = path to the grid file that contains the dataset
                      to be plotted.

Defaults : output FILEPATH is constructed from grid name and data_key.
           IMAGE_FORMAT = png.
           TITLE = constructed from grid name and data_key 
"""

import os, sys
import datetime
import numpy as N

import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot

from nrcc.utils.options import getBboxFromOptions, getDataBboxFromOptions
from nrcc.utils.report import Reporter
from nrcc.utils.units import convertUnits
from nrcc.base.manager import DatasetKey

from nrcc.stations.scripts.factory import StationDataManagerFactory

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

#APP = os.path.splitext(os.path.split(__file__)[1])[0].upper().replace('_',' ')
APP = os.path.split(sys.argv[0])[1] + ' ' + ' '.join(sys.argv[1:])
#PID = os.getpid()
PID = 'PID %d' % os.getpid()

UNITS = { 'C' : chr(176) +'C'
        , 'K' : chr(176) +'K'
        , 'F' : chr(176) +'F'
        }

temp_colors = ('#660099','#A000C8','#1E3EFF','#00C8C8','#00DC00','#A0E632',
               '#E6DC32','#E6AF2D','#F08228','#FF0000')

elev_colors = ('#FFFFFF','#A000C8','#1E3EFF','#00C8C8','#00DC00','#A0E632',
               '#E6DC32','#E6AF2D','#F08228','#FF0000')

more_colors = ('#999999',
               '#660099','#A000C8','#00C8C8','#0099FF','#1E3EFF','#009900',
               #'#660099','#A000C8','#00C8C8','#0099FF','#1E3EFF','#DDDDDD',
               '#00DC00','#A0E632','#E6DC32','#E6AF2D','#F08228','#FF0000')
#               '#000000')


ELEVATIONS = [ {} for indx in range(5) ]
ELEVATIONS[0] = { 'discrete' : (-1000.,.01,50.,100.,200.,300.,400.,500.,600.,
                                700.,1000.) ,
                  'ticks' : (.01,50.,100.,200.,300.,400.,500.,600.,700.) }
ELEVATIONS[1] = { 'discrete' : (-100.,0.1,100.,200.,400.,600.,800.,1000.,1100.,
                                1200.,1300.,1400.,1500.,2000.) ,
                  'ticks' : (0.1,100.,200.,400.,600.,800.,1000.,1100.,1200.,
                             1300.,1400.,1500.) }
ELEVATIONS[2] = { 'discrete' : (-100.,0.1,100.,200.,400.,600.,800.,1000.,1250.,
                                1500.,1750., 2000.,2500.,5000.) ,
                  'ticks' : (0.1,100.,200.,400.,600.,800.,1000.,1250.,1500.,
                             1750.,2000.,2500.) }
ELEVATIONS[3] = { 'discrete' : (-100.,250.,500.,750.,1000.,1250.,1500.,1750.,
                              2000.,2250.,2500.,2750.,3000.,6000.) ,
                  'ticks' : (250.,500.,750.,1000.,1250.,1500.,1750.,2000.,
                             2250.,2500.,2750.,3000.) }
ELEVATIONS[4] = { 'discrete' : (0.,1000.,1500.,2000.,2500.,3000.,3500., 4000.,
                                5000.,6000.,7000.,8000.,9000.,15000.) ,
                  'ticks' : (1000.,1500.,2000.,2500.,3000.,3500.,4000.,5000.,
                             6000.,7000.,8000.,9000.) }

PLOT_BBOX = { 'CONUS' : (-124.75, 24., -66.9, 49.5),
              'EOR'   : (-112., 24., -65., 50.),
              'HP'    : (-111., 37., -95.5, 49.),
              'MW'    : (-97.4, 36., -80.5, 49.5),
              'NE'    : (-85., 34., -65., 50.),
              'SE'    : (-89., 24, -75.4, 39.5),
              'SO'    : (-106.5, 25.7, -88., 37.),
              'W'     : (-125., 30.5, -103., 49.5),
            }


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def calculateTicks(first_tick, last_tick, num_ticks):
    per_tick = int( ( ((last_tick-first_tick)+0.5) / num_ticks) + 0.5)
    return [first_tick+(tick*per_tick) for tick in range(num_ticks)]

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def getTicksFromArgs(args, num_ticks, data_min, data_max, units,
                     always_calc=False):
    if len(args) > 4:
        if '-' in args[4]:
            arg = args[4].replace('\\','')
            first_tick = eval(arg)
        else:
            first_tick = float(args[4])
    else:
        if always_calc:
            first_tick = round(data_min + 2.5)
        else:
            return None, None

    if len(args) > 5:
        if args[5].isdigit():
            per_tick = int(args[5])
        else:
            per_tick = float(args[5])
    else:
        last_tick = round(data_max - 0.5)
        per_tick = int( ( ((last_tick-first_tick)+0.5) / num_ticks) + 0.5)

    ticks = [first_tick+(tick*per_tick) for tick in range(num_ticks)]
    if units == 'K':
        discrete = [-250.,] + ticks + [350.,]
    elif units == 'F':
        discrete = [-32.,] + ticks + [130.,]
    elif units == 'm':
        discrete = [-30.,] + ticks + [7500.,]
    elif units == 'ft':
        discrete = [-100.,] + ticks + [22000.,]
    else:
        extreme = 20.*per_tick
        discrete = [ticks[0] - extreme,] + ticks + [ticks[-1] + extreme,]

    return ticks, discrete


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser(usage=USAGE)
parser.add_option('-b', action='store', type='string', dest='bbox',
                  default=None)
parser.add_option('-c', action='store_true', dest='extended_colors',
                  default=False)
parser.add_option('-d', action='store', type='string', dest='diff',
                  default=None)
parser.add_option('-e', action='store', type='int', dest='elevations',
                  default=0)
parser.add_option('--db', action='store_true', default=False, dest='data_bbox')
parser.add_option('--fi', action='store', type='float', dest='inf_fill_value',
                  default=None)
parser.add_option('--fn', action='store', type='float', dest='nan_fill_value',
                  default=None)
parser.add_option('-o', action='store', type='string', dest='output_filepath',
                  default=None, help='full path for the plot image file')
parser.add_option('-s', action='store', type='string', dest='source',
                   default='dem5k', help=' Data source.')
parser.add_option('--ss', action='store', type='int', dest='symbol_size',
                  default=None)
parser.add_option('--sum', action='store', type='string', dest='sum',
                  default=None)
parser.add_option('-t', action='store', default=None, dest='title',
                  help='plot title (be sure it is enclosed in double quotes)')
parser.add_option('-u', action='store', type='string', dest='units',
                  default=None)
parser.add_option('-x', action='store_false', default=True, dest='set_ticks')

# processing script configuration options
parser.add_option('-f', action='store', type='string', dest='raw_data_format',
                  default=None,
                  help='format of downloaded data : dods, grib, etc.')
parser.add_option('-i', action='store', type='string', dest='grid_node_intvl',
                  default=None,
                  help='interval bewtween nodes in downloaded data grids')
parser.add_option('-m', action='store', type='string', dest='source_model',
                  default=None,
                  help='model that supplies downloaded data : rap, ruc, etc.')
parser.add_option('-r', action='store', type='string', dest='region',
                  default=None,
                  help="name of NOAA/RCC region to process : ne, eor, etc.")

# files and directories
parser.add_option('-l', action='store', type='string', dest='log_filepath',
                  default=None,
                  help='path to alternate file to be used for logging')
parser.add_option('-w', action='store', type='string', dest='working_dir',
                  default=None,
                  help="alternate working directory for downloaded files")
# debug
parser.add_option('-z', action='store_true', dest='debug', default=False,
                  help='show all available debug output')

options, args = parser.parse_args()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

data_key = args[0]

if len(args) > 1:
    year = int(args[1])
    month = int(args[2])
    day = int(args[3])
else:
    today = datetime.datetime.now()
    year = today.year
    month = today.month
    day = today.day
date = (year,month,day)

if '[' in data_key:
    data_key, data_slice = data_key.split('[')
    data_slice = data_slice[:data_slice.find(']')]
    if data_slice in ('j','J'):
        data_slice = datetime.datetime(*date).timetuple().tm_yday - 1
    elif data_slice.isdigit():
        data_slice = int(data_slice)
    else:
        data_slice = eval(data_slice)
    dataset_key = DatasetKey(data_key, slice=data_slice)
else:
    dataset_key = DatasetKey(data_key)

reporter = Reporter(PID, options.log_filepath)
reporter.reportEvent(APP)
reporter.close()

title_format = '(%s)'
if options.diff is not None:
    data_key_string = '%s-%s' % (data_key, options.diff)
elif options.sum is not None:
    data_key_string = '%s+%s' % (data_key, options.sum)
else:
    data_key_string = data_key
    if '-' not in data_key:
        title_format = '%s'

diff_or_bias = (options.diff is not None or 'diff' in data_key
                or '_bias' in data_key or '-' in data_key) 
per_degree = ('diff' in data_key or '_bias' in data_key
              or '-' in data_key or 'lapse' in data_key)

set_ticks = options.set_ticks
extended_colors = options.extended_colors
use_data_bbox = options.data_bbox

factory = StationDataManagerFactory(date, options)

region_name = factory.region_name
if use_data_bbox:
    plot_bbox = factory.region.data_bbox
else:
    plot_bbox = factory.region.plot_bbox
if region_name == 'W':
    extended_colors = True

if 'elev' in data_key:
    units = 'm'
else:
    units = 'K'

source = 'stn'
prefix = 'STN-%s' % region_name
if region_name in ('EOR','CONUS'):
    symbol_size = 10
else:
    symbol_size = 40

suffix = ''
if use_data_bbox:
    bbox = getDataBboxFromOptions(options)
else:
    bbox = getBboxFromOptions(options)

if bbox is not None:
    suffix = '-bbox'
    plot_bbox = bbox
else:
    bbox = factory.region_bbox

working_dir = factory.getDirectory('working')
data_filepath = factory.getFilepath('stations')

if not os.path.isfile(data_filepath):
    errmsg = "Data file was not found : '%s'"
    raise IOError, errmsg % data_filepath


# get data_manager and data for the first date
ManagerClass = factory.getDataManagerClass(source)
manager = ManagerClass(data_filepath)
manager.setLonLatBounds(bbox)
lons, lats = manager.getLonLat()
data, attrs = manager.getRawData(dataset_key)
units = attrs.get('units',units)

# can't handle integer arrays !!!
if data.dtype in (N.int64, N.int32, N.int16, N.int8,
                  N.uint64, N.uint32, N.uint16, N.uint8):
    data = N.array(data, dtype=float)
    data[data < -32767.] = N.nan
    data[data < -998.] = N.inf
 

if options.diff is not None:
    other_dataset_name = options.diff
elif options.sum is not None:
    other_dataset_name = options.sum
else:
    other_dataset_name = None

if other_dataset_name is not None:
    colon = other_dataset_name.find(':')
    if colon > 0:
        src = other_dataset_name[:colon]
        other_dataset_name = other_dataset_name[colon+1:]
        data_filepath = dataFilepath(src, other_dataset_name, region_name,
                                     working_dir, date)
        ManagerClass = dataManager(src)
        manager = ManagerClass(data_filepath)
    if options.units is not None:
        other_data, other_attrs = manager.getData(other_dataset_name,
                                                  units=units)
    else:
        other_data, other_attrs = manager.getData(other_dataset_name)
    if options.diff is not None:
        data = data - other_data
    else:
        data = data + other_data

if options.units is not None:
    from_units = units
    to_units = options.units
    if diff_or_bias and from_units in ('K','C','F'):
        from_units = 'd' + from_units
        to_units = 'd' + to_units
    data = convertUnits(data, from_units, to_units)
    units = options.units

if 'elev' in data_key and diff_or_bias:
    data[N.where((data < 0.1) & (data > -0.1))] = N.nan

data_min = data[N.isfinite(data)].min()
data_max = data[N.isfinite(data)].max()
reporter.logInfo('data min = %s   max = %s' % (str(data_min),str(data_max)))

if options.inf_fill_value is not None:
    data[N.where(N.isinf(data))] = options.inf_fill_value

if options.nan_fill_value is not None:
    data[N.where(N.isnan(data))] = options.nan_fill_value

if set_ticks:
    if extended_colors:
        num_ticks = len(more_colors) - 1
    else:
        num_ticks = len(temp_colors) - 1

    if diff_or_bias:
        if 'elev' in data_key:
            discrete = (-1000.,-500.,-200.,-100.,-50.,-20.,-10.,10.,20.,50.,
                         100.,200.,500.,1000.)
            ticks = (-500.,-200.,-100.,-50.,-20.,-10.,10.,20.,50.,100.,
                      200.,500.)
        else:
            if data_max > 40:
                if data_min > -5:
                    ticks = (-1.,0.,1.,2.5,5.,10.,20.,30.,40.)
                elif data_min > -15:
                    ticks = (-5.,-2.5,0.,2.5,5.,10.,20.,30.,40.)
                elif data_min > -30:
                    ticks = (-20.,-10.,-5.,0.,5.,10.,20.,30.,40.)
                discrete = list(ticks)
                discrete.insert(0,-100.)
                discrete.append(100.)
            elif data_max > 20:
                if data_min > -5:
                    ticks = (-2.5,-1.,0.,1.,2.5,5.,10.,15.,20.)
                elif data_min > -15:
                    ticks = (-10.,-5.,-2.5,0.,2.5,5.,10.,15.,20.)
                elif data_min > -30:
                    ticks = (-20.,-15.,-10.,-5.,0.,5.,10.,15.,20.)
                discrete = list(ticks)
                discrete.insert(0,-100.)
                discrete.append(100.)
            elif data_min < -40:
                if data_max <= 10.:
                    ticks = (-40.,-30.,-20.,-10.,-5.,-1,0.,1.,5.)
                elif data_max < 20.:
                    ticks = (-40.,-30.,-20.,-10.,-5.,0.,5.,10.,15.)
                else:
                    ticks = (-40.,-30.,-20.,-10.,0.,10.,20.,30.,40.)
                discrete = list(ticks)
                discrete.insert(0,-100.)
                discrete.append(100.)
            else:
                discrete = (-50.,-10.,-5.,-1.,0.,1.,2.5,5.,7.,10.,50.)
                ticks = (-10.,-5.,-1.,0.,1.,2.5,5.,7.,10.)
    elif 'elev' in data_key:
        ticks, discrete = getTicksFromArgs(args, num_ticks, data_min, data_max,
                                           units, False)
        if ticks is None:
            discrete = ELEVATIONS[options.elevations]['discrete']
            ticks = ELEVATIONS[options.elevations]['ticks']
    elif 'lapse' in data_key:
        discrete = (-10., -1., -0.1, -0.05, -0.03, -0.02, -0.01, 0.01, 0.02,
                    0.03, 0.05, 0.1, 1., 10.)
        ticks = (-1., -0.1, -0.05, -0.03, -0.02, -0.01, 0.01, 0.02, 0.03, 0.05,
                 0.1, 1.)
    elif 'mask' not in data_key:
        ticks, discrete = getTicksFromArgs(args, num_ticks, data_min, data_max,
                                           units, True)
    elif data_key in ('lon','lat'):
        ticks = calculateTicks(int(data_min+0.5)+1, int(data_max-0.5)-1, 12)
        discrete = ticks
        discrete.insert(0,int(data_min-3.))
        discrete.append(int(data_max+3.))
        print discrete

    if 'elev' in data_key:
        if extended_colors or diff_or_bias or options.elevations > 0:
            cmap = matplotlib.colors.ListedColormap(more_colors)
            norm = matplotlib.colors.BoundaryNorm(discrete, len(discrete)-1)
        else:
            cmap = matplotlib.colors.ListedColormap(elev_colors)
            norm = matplotlib.colors.BoundaryNorm(discrete, len(elev_colors))
    elif 'lapse' in data_key:
        cmap = matplotlib.colors.ListedColormap(more_colors)
        norm = matplotlib.colors.BoundaryNorm(discrete, len(discrete)-1)
    elif 'mask' in data_key:
        colors = ('#FFFFFF','#666666','#CCCCFF','#FFFFFF')
        cmap = matplotlib.colors.ListedColormap(colors)
        ticks = (0.,1.)
        discrete = (-1000000.,0.,1.,1000000.)
        norm = matplotlib.colors.BoundaryNorm(discrete,len(colors))
    else:
        if extended_colors:
            cmap = matplotlib.colors.ListedColormap(more_colors)
            norm = matplotlib.colors.BoundaryNorm(discrete,len(more_colors))
        else:
            cmap = matplotlib.colors.ListedColormap(temp_colors)
            norm = matplotlib.colors.BoundaryNorm(discrete,len(temp_colors))


subtitle = None
if options.title is None:
    data_title = title_format % data_key_string
    title = prefix + ' ' + data_title
    title += ' (%s to %s' % ( ('%6.0f' % data_min).strip(),
                              ('%6.0f' % data_max).strip() )
    units = str(units)
    if units is not None:
        title += ' %s)' % UNITS.get(units, units).decode('latin1') 
    else:
        title += ')'
else:
    title = options.title
    if ':' in title:
        colon = title.find(':')
        subtitle = title[colon+1:].strip()
        title = title[:colon]

if subtitle is None:
    _date = datetime.datetime(*date)
    subtitle = _date.strftime('%B') 
    subtitle += ' %d' % _date.day
    subtitle += ', %d' % _date.year
    if source == 'stn':
        subtitle += ' (%d stations)' % lons.size


if options.symbol_size is not None:
    symbol_size = options.symbol_size
figure = pyplot.figure(figsize=(8,6),dpi=100)
pyplot.xlim(plot_bbox[0],plot_bbox[2])
pyplot.ylim(plot_bbox[1],plot_bbox[3])
#axes = fig.add_subplot(111)
axis = pyplot.gca()
if set_ticks:
    scatter = axis.scatter(lons, lats, c=data, marker='s', cmap=cmap,
                           norm=norm, s=symbol_size, linewidths=0)
else:
    scatter = axis.scatter(lons, lats, c=data, marker='s', cmap='jet',
                           s=symbol_size, linewidths=0)
#ticks = arange(-0.06, 0.061, 0.02)
#xticks(ticks)
#yticks(ticks)
#axis.xaxis.set_data_interval(-85.,-65.)
#axis.yaxis.set_data_interval(34.,50.)
axis.set_xlabel('Longitude', fontsize=20)
axis.set_ylabel('Latitude', fontsize=20)
axis.grid(True)

ax_pos = axis.get_position()
l,b,w,h = ax_pos.bounds
color_axis = pyplot.axes([l+w, b, 0.025, h])

if set_ticks:
    pyplot.colorbar(scatter, cax=color_axis, ticks=ticks)

pyplot.axes(axis)

pyplot.suptitle(title, fontsize=16)
pyplot.title(subtitle, fontsize=12)

if options.output_filepath is None:
    filename = prefix + '-%d%02d%02d-plot-' % date
    filename += data_key_string + '-' + units + suffix
    filename += '.png'
    output_filepath =  os.path.join(working_dir, filename)
else:
    output_filepath = options.output_filepath

reporter.logInfo('Saving %s plot to %s' % (data_key_string, output_filepath))
figure.savefig(output_filepath)
reporter.logEvent('process ended successfully')

