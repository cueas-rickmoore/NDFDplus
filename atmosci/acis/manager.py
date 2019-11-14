
from copy import deepcopy

import numpy as N

from cuas.base.array import GeoArrayFileManager
from cuas.hdf5.manager import HDF5DataFileMixin

from cuas.stations import elements

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

MORNING_OBS_TIMES = (4,5,6,7,8,9)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class StationDataFileManager(HDF5DataFileMixin, GeoArrayFileManager):

    DATA_TYPES = deepcopy(elements.DATA_TYPES)
    DATA_UNITS = deepcopy(elements.DATA_UNITS)
    DESCRIPTIONS = deepcopy(elements.DESCRIPTIONS)
    MASKED = deepcopy(elements.MASKED)
    MISSING = deepcopy(elements.MISSING)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def getStationsWhere(self, dataset_key, value):
        dataset, units = self.getRawData(dataset_key)
        if N.isnan(value):
            return dataset[N.where(N.isnan(dataset))]
        elif N.isposinf(value):
            return dataset[N.where(N.isposinf(dataset))]
        elif N.isneginf(value):
            return dataset[N.where(N.isneginf(dataset))]
        return dataset[N.where(dataset == value)]

