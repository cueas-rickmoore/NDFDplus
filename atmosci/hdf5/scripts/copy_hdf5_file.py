#! /Volumes/Transport/venv2/ndfd/bin/python

import os, sys

from atmosci.hdf5.file import Hdf5FileReader, Hdf5FileManager

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from optparse import OptionParser
parser = OptionParser()
parser.add_option('-x', action='store_true', dest='replace_existing',
                  default=False)
options, args = parser.parse_args()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import datetime
from dateutil.relativedelta import relativedelta
target_date = datetime.date.today() - relativedelta(days=1)
last_obs_date = target_date - relativedelta(days=1)

from_filepath = os.path.normpath(args[0])
to_filepath = from_filepath.replace('.h5','-COPY.h5')

if os.path.exists(to_filepath):
    replace_existing = options.replace_existing
    if replace_existing:
        os.remove(to_filepath)
    else:
        print to_filepath, 'already exists.'
        exit(1)

msg = "Copying datasets from '%s'\nto new file at '%s'" 
print msg % (from_filepath, to_filepath)
reader = Hdf5FileReader(from_filepath)

manager = Hdf5FileManager(to_filepath, 'a')
manager.setFileAttributes(**dict(reader.getFileAttributes()))

for name in reader.group_names:
    attrs = reader.getGroupAttributes(name)
    print "creating '%s' group" % name
    manager.open('a')
    manager.createGroup(name)
    manager.setGroupAttributes(name, **attrs)
    manager.close()

for name in reader.dataset_names:
    print "creating '%s' dataset" % name
    dataset = reader.getDataset(name)
    chunks = dataset.chunks
    attrs = dict(dataset.attrs)
    manager.open('a')
    manager.createDataset(name, dataset, chunks=chunks)
    manager.setDatasetAttributes(name, **attrs)
    manager.close()

reader.close()

