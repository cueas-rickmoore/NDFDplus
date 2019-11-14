#! /Volumes/Transport/venv2/ndfd/bin/python

import os, sys
import datetime
import numpy as N

from atmosci.utils.options import stringToTuple
from atmosci.hdf5.manager import HDF5DataFileManager
from atmosci.acis.griddata import AcisGridDataClient

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

APP = os.path.split(sys.argv[0])[1] + ' ' + ' '.join(sys.argv[1:])
PID = 'PID %d' % os.getpid()

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
#metadata = stringToTuple(options.metadata)
wait_time = options.wait_time

if len(args) > 0:
    year = int(args[0])
    month = int(args[1])
    day = int(args[2])
    date = (year,month,day)
else:
    date = datetime.date.today().timetuple()[:3]

grid = args[3]

elements = [ ]
elem_names = [ ]
for elem_name in args[4:]:
    elements.append({"name":"%s" % elem_name})
    elem_names.append(elem_name)

request = { "bbox":"%s" % options.bbox,
            "date":"%d%02d%02d" % date,
            "grid":"%s" % grid,
            "elems":elements
          }

if options.base_url is None:
    client = AcisGridDataClient()
else: client = AcisGridDataClient(options.base_url)
data = client.jsonToPython(*client.query(request))
grid = N.array(data['data'][0][1], dtype=float)
grid[grid == -999] = N.nan
print grid[:2]

filename = '%s_acis_%s_grid.h5' % (request['date'], elem_names[0])
manager = HDF5DataFileManager(filename, 'w')
manager.createDataset(elem_names[0], grid)
manager.closeFile()

