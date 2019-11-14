#! /Volumes/Transport/venv2/ndfd/bin/python

import os

from atmosci.acis import ACIS_DIRPATH
from atmosci.acis.nodefinder import AcisGridNodeIndexer

from atmosci.hdf5.grid import Hdf5GridFileReader

static_filepath = os.path.join(ACIS_DIRPATH, 'data', 'acis5k_NE_static.h5')
reader = Hdf5GridFileReader(static_filepath)
lat = reader.getData('lat')
lon = reader.getData('lon')
spacing = reader.datasetAttribute('lat','node_spacing')
reader.close()

offset = spacing * 0.2

node_indexer = AcisGridNodeIndexer('NE')

for i in range(lat.shape[0]):
    olat = lat[i,i] + offset
    olon = lon[i,i] + offset

    y, x, flon, flat = node_indexer(olon, olat, 3, False)

    print '\n'
    print lat[i,i], olat, flat, y, lat[x, y]
    print lon[i,i], olon, flon, x, lon[x, y]

