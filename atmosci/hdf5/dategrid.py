""" Classes for accessing data from Hdf5 encoded grid files.
"""

import os
import datetime

import numpy as N

from atmosci.utils.timeutils import asDatetime, asDatetimeDate
from atmosci.utils.timeutils import dateAsString, matchDateType

from atmosci.hdf5.grid import Hdf5GridFileReader, Hdf5GridFileManager
from atmosci.hdf5.grid import Hdf5GridFileBuilder

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

TIME_SPAN_ERROR = 'Invalid time span. '
TIME_SPAN_ERROR += 'Either "date" OR "start_date" plus "end_date" are required.'

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

#class Hdf5DateGridReaderMixin(object):
class Hdf5DateGridReaderMixin:

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    
    def dataAtNode(self, dataset_path, lon, lat, start_date=None,
                         end_date=None, **kwargs):
        y, x = self.ll2index(lon, lat)
        if start_date is None:
            data = self.getDataset(dataset_path).value[:, y, x]
        else:
            if end_date is None:
                indx = self.indexForDate(dataset_path, date)
                data = self.getDataset(dataset_path).value[indx, y, x]
            else:
                start, end = \
                self.indexesForDates(dataset_path, start_date, end_date)
                data = self.getDataset(dataset_path).value[start:end, y, x]
        return self._processDataOut(dataset_path, data, **kwargs)
    getDataAtNode = dataAtNode # backwards compatibility

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def dataForDate(self, dataset_path, date, **kwargs):
        indx = self.indexForDate(dataset_path, date)
        dataset = self.getDataset(dataset_path)
        return self._processDataOut(dataset_path, dataset[indx], **kwargs)
    getDataForDate = dataForDate # backwards compatibility
    
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def dataSince(self, dataset_path, date, **kwargs):
        return self.getDateSlice(dataset_path, date, self.end_date, **kwargs)
    getDataSince = dataSince # backwards compatibility

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def dataThru(self, dataset_path, date, **kwargs):
        return self.getDateSlice(dataset_path, self.start_date, date, **kwargs)
    getDataThru = dataThru # backwards compatibility

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def dateAttribute(self, object_path, attribute_name, default=None):
        date = self.getObjectAttribute(object_path, attribute_name, default)
        if date != default: return asDatetimeDate(date)
        return default
    getDateAttribute = dateAttribute # backwards compatibility

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def dateAttributes(self, object_path=None, as_date_obj=False):
        if object_path is None:
            attrs = self.fileAttributes()
        else: attrs = self.objectAttributes(object_path)
        date_attrs = {}
        if as_date_obj:
            for key, value in attrs.items():
                if key.endswith('date'):
                    date_attrs[key] = asDatetimeDate(value)
        else:
            for key, value in attrs.items():
                if key.endswith('date'): date_attrs[key] = value
        return date_attrs
    getDateAttributes = dateAttributes # backwards compatibility

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def dateSlice(self, dataset_path, start_date, end_date, **kwargs):
        start, end = self.indexesForDates(dataset_path, start_date, end_date)
        dataset = self.getDataset(dataset_path)
        data = self._dateSlice(dataset, start, end)
        return self._processDataOut(dataset_path, data, **kwargs)
    getDateSlice = dateSlice # backwards compatibility

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def dataSlice(self, dataset_path, start_date, end_date, min_lon, max_lon,
                         min_lat, max_lat, **kwargs):
        min_y, min_x = self.ll2index(min_lon, min_lat)
        max_y, max_x = self.ll2index(max_lon, max_lat)
        start, end = self.indexesForDates(dataset_path, start_date, end_date)
        dataset = self.getDataset(dataset_path)
        data = \
        self._slice3DDataset(dataset, start, end, min_y, max_y, min_x, max_x)
        return self._processDataOut(dataset_path, data, **kwargs)
    get3DSlice = dataSlice # backwards compatibility

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def indexForDate(self, dataset_path, date_obj):
        if isinstance(date_obj, datetime.datetime): date = date_obj.date()
        elif isinstance(date_obj, datetime.date): date = date_obj
        else:
            errmsg = 'Invalid type for "date_obj" argument : %s'
            raise TypeError, errmsg % str(type(date_obj))

        start_date = \
            self.dateAttribute(dataset_path, 'start_date', self.start_date)
        end_date = self.dateAttribute(dataset_path, 'end_date', self.end_date)

        if date >= start_date and date <= end_date:
            return (date - start_date).days
        else:
            errmsg = '%s is outside the valid range' % str(date)
            errmsg = '%s (%%s to %%s) for "%%s" dataset' % errmsg
            errmsg = errmsg % (str(start_date), str(end_date), dataset_path)
            raise ValueError, errmsg
    indexFromDate = indexForDate # backwards compatibility
    _indexForDate = indexForDate # backwards compatibility

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def indexesForDates(self, dataset_path, start_date, end_date):
        start_index = self.indexForDate(dataset_path, start_date)
        if end_date is None: end_index = start_index + 1
        else: end_index = self.indexForDate(dataset_path, end_date) + 1
        return (start_index, end_index)
    indexesFromDates = indexesForDates # backwards compatibility
    _indexesForDates = indexesForDates # backwards compatibility

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def setDateAttribute(self, object_path, attribute_name, date):
        if isinstance(date, basestring):
            self.setObjectAttribute(object_path, attribute_name, date)
        else:
            date_str = date.strftime('%Y-%m-%d')
            self.setObjectAttribute(object_path, attribute_name, date_str)

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _dateSlice(self, dataset, start_index, end_index):
        dataset_dims = len(dataset.shape)
        if end_index == start_index:
            if dataset_dims == 1: return dataset[start_index]
            elif dataset_dims == 2: return dataset[start_index, :]
            else: return dataset[start_index, :, :]
        else:
            if dataset_dims == 1: return dataset[start_index:end_index]
            elif dataset_dims == 2:
                return dataset[start_index:end_index, :]
            else:
                return dataset[start_index:end_index, :, :]

   # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _slice3DDataset(self, dataset, start, end, min_y, max_y, min_x, max_x):
        if end == start: # single date
            if max_y == min_y:
                if max_x == min_x: # retrieve data for one node
                    return dataset[start, min_y, min_x]
                elif max_x < dataset.shape[2]:
                    return dataset[start, min_y, min_x:max_x]
                else: # max_x >= dataset.shape[2]
                    return dataset[start, min_y, min_x:]
            elif max_y < dataset.shape[1]:
                if max_x == min_x:
                    return dataset[start, min_y:max_y, min_x]
                elif max_x < dataset.shape[2]:
                    return dataset[start, min_y:max_y, min_x:max_x]
                else: # max_x >= dataset.shape[2]
                    return dataset[start, min_y:max_y, min_x:]
            else: # max_y >= dataset.shape[1]
                if max_x == min_x:
                    return dataset[start, min_y:, min_x]
                elif max_x < dataset.shape[2]:
                    return dataset[start, min_y:, min_x:max_x]
                else: # max_x >= dataset.shape[2]
                    return dataset[start, min_y:, min_x:]
        elif end < dataset.shape[0]:
            if max_y == min_y:
                if max_x == min_x: # retrieve data for one node
                    return dataset[start:end, min_y, min_x]
                elif max_x < dataset.shape[2]:
                    return dataset[start:end, min_y, min_x:max_x]
                else: # max_x >= dataset.shape[2]
                    return dataset[start:end, min_y, min_x:]
            elif max_y < dataset.shape[1]:
                if max_x == min_x:
                    return dataset[start:end, min_y:max_y, min_x]
                elif max_x < dataset.shape[2]:
                    return dataset[start:end, min_y:max_y, min_x:max_x]
                else: # max_x >= dataset.shape[2]
                    return dataset[start:end, min_y:max_y, min_x:]
            else: # max_y >= dataset.shape[1]
                if max_x == min_x:
                    return dataset[start:end, min_y:, min_x]
                elif max_x < dataset.shape[2]:
                    return dataset[start:end, min_y:, min_x:max_x]
                else: # max_x >= dataset.shape[2]
                    return dataset[start:end, min_y:, min_x:]
        else: # end > dataset.shape[0] ... retrieve all dates to end of dataset
            if max_y == min_y:
                if max_x == min_x: # retrieve data for one node
                    return dataset[start:, min_y, min_x]
                elif max_x < dataset.shape[2]:
                    return dataset[start:, min_y, min_x:max_x]
                else: # max_x >= dataset.shape[2]
                    return dataset[start:, min_y, min_x:]
            elif max_y < dataset.shape[1]:
                if max_x == min_x:
                    return dataset[start:, min_y:max_y, min_x]
                elif max_x < dataset.shape[2]:
                    return dataset[start:, min_y:max_y, min_x:max_x]
                else: # max_x >= dataset.shape[2]
                    return dataset[start:, min_y:max_y, min_x:]
            else: # max_y >= dataset.shape[1]
                if max_x == min_x:
                    return dataset[start:, min_y:, min_x]
                elif max_x < dataset.shape[2]:
                    return dataset[start:, min_y:, min_x:max_x]
                else: # max_x >= dataset.shape[2]
                    return dataset[start:, min_y:, min_x:]

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _loadDateGridAttributes_(self):
        start_date = self.fileAttribute('start_date', None)
        if start_date is not None: self.start_date = asDatetimeDate(start_date)
        end_date = self.fileAttribute('end_date', None)
        if end_date is not None: self.end_date = asDatetimeDate(end_date)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class Hdf5DateGridManagerMixin(Hdf5DateGridReaderMixin):
    """ Provides read/write access to 3D gridded data in HDF5 files where
    the first dimension is time, the 2nd dimension is rows and the 3rd
    dimension is columns. Grids can contain any type of data. Indexing
    based on Time/Latitude/Longitude is available. Time indexes may be
    derived from date/time with earliest date at index 0. Row indexes
    may be derived from Latitude coordinates with minimum Latitude at
    row index 0. Columns indexes may be derived from Longitude
    coordinates with minimum Longitude at column index 0.

    Inherits all of the capabilities of Hdf5GridFileManager
    """

    def __init__(self, hdf5_filepath, mode='r'):
        Hdf5GridFileManager.__init__(self, hdf5_filepath, mode)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def insertByDate(self, dataset_path, data, start_date, **kwargs):
        date_index = self.indexForDate(dataset_path, start_date)
        self._insertByDateIndex(dataset_path, data, date_index, **kwargs)

    def insertAtNodeByDate(self, dataset_path, data, start_date, lon, lat,
                                 **kwargs):
        date_index = self.indexForDate(dataset_path, start_date)
        y, x = self.ll2index(lon, lat)
        self._insertDateAtNodeByDateIndex(dataset_path, data, date_index, x, y,
                                          **kwargs)

    def insert3DSlice(self, dataset_path, data, start_date, min_lon, max_lon,
                            min_lat, max_lat, **kwargs):
        min_y, min_x = self.ll2index(min_lon, min_lat)
        max_y, max_x = self.ll2index(max_lon, max_lat)
        date_index = self.indexForDate(dataset_path, start_date)
        self._insert3DSlice(dataset_path, data, date_index, min_y, max_y,
                            min_x, max_x, **kwargs)

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _insertByDateIndex(self, dataset_path, data, date_index, **kwargs):
        if not isinstance(data, N.ndarray):
            errmsg = 'Data for "%s" dataset must be a NumPy array'
            raise TypeError, errmsg % dataset_path
        errmsg = 'Cannot insert data with %dD data into %dD dataset by date.'
        data_in = self._processDataIn(dataset_path, data, **kwargs)

        dataset = self.getDataset(dataset_path)
        dataset_dims = len(dataset.shape)
        if dataset_dims == 3:
            if data_in.ndim == 3:
                end_index = date_index + data.shape[0]
                dataset[date_index:end_index] = data_in
            elif data.ndim == 2:
                dataset[date_index,:,:] = data_in
            else:
                raise ValueError, errmsg % (data.ndim, dataset_dims)
        elif dataset_dims == 1:
            if isinstance(data_in, N.ndarray):
                if data_in.ndim == 1:
                    if len(data_in) > 1:
                        end_index = date_index + len(data)
                        dataset[date_index:end_index] = data_in
                    else:
                        dataset[date_index] = data_in
                else:
                    raise ValueError, errmsg % (data.ndim, dataset_dims)
            else: # insert scalar value
                dataset[date_index] = data_in
        else:
            raise ValueError, errmsg % (data.ndim, dataset_dims)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _insertAtNodeByDateIndex(self, dataset_path, data, date_index, x, y,
                                       **kwargs):
        end_index = date_index + data.shape[0]
        dataset = self.getDataset(dataset_path)
        dataset[date_index:end_index, y, x] = \
            self._processDataIn(dataset_path, data, **kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _insert3DSlice(self, dataset_path, data, start, min_y, max_y,
                             min_x, max_x):
        if data.dims == 2: end = start
        elif data.dims == 3: end = start + data.shape[0]
        else:
            errmsg = 'Cannot insert %dD data into a 3D dataset.'
            raise ValueError, errmsg % data.dims

        dataset = self.getDataset(dataset_path)
        if end == start: # single date
            if max_y == min_y:
                if max_x == min_x: # retrieve data for one node
                    dataset[start, min_y, min_x] = data
                elif max_x < dataset.shape[2]:
                    dataset[start, min_y, min_x:max_x] = data
                else: # max_x >= dataset.shape[2]
                    dataset[start, min_y, min_x:] = data
            elif max_y < dataset.shape[1]:
                if max_x == min_x:
                    dataset[start, min_y:max_y, min_x] = data
                elif max_x < dataset.shape[2]:
                    dataset[start, min_y:max_y, min_x:max_x] = data
                else: # max_x >= dataset.shape[2]
                    dataset[start, min_y:max_y, min_x:] = data
            else: # max_y >= dataset.shape[1]
                if max_x == min_x:
                    dataset[start, min_y:, min_x] = data
                elif max_x < dataset.shape[2]:
                    dataset[start, min_y:, min_x:max_x] = data
                else: # max_x >= dataset.shape[2]
                    dataset[start, min_y:, min_x:] = data
        elif end < dataset.shape[0]:
            if max_y == min_y:
                if max_x == min_x: # retrieve data for one node
                    dataset[start:end, min_y, min_x] = data
                elif max_x < dataset.shape[2]:
                    dataset[start:end, min_y, min_x:max_x] = data
                else: # max_x >= dataset.shape[2]
                    dataset[start:end, min_y, min_x:] = data
            elif max_y < dataset.shape[1]:
                if max_x == min_x:
                    dataset[start:end, min_y:max_y, min_x] = data
                elif max_x < dataset.shape[2]:
                    dataset[start, min_y:max_y, min_x:max_x] = data
                else: # max_x >= dataset.shape[2]
                    dataset[start:end, min_y:max_y, min_x:] = data
            else: # max_y >= dataset.shape[1]
                if max_x == min_x:
                    dataset[start:end, min_y:, min_x] = data
                elif max_x < dataset.shape[2]:
                    dataset[start:end, min_y:, min_x:max_x] = data
                else: # max_x >= dataset.shape[2]
                    dataset[start:end, min_y:, min_x:] = data
        else: # end > dataset.shape[0] ... retrieve all dates to end of dataset
            if max_y == min_y:
                if max_x == min_x: # retrieve data for one node
                    dataset[start:, min_y, min_x] = data
                elif max_x < dataset.shape[2]:
                    dataset[start:, min_y, min_x:max_x] = data
                else: # max_x >= dataset.shape[2]
                    dataset[start:, min_y, min_x:] = data
            elif max_y < dataset.shape[1]:
                if max_x == min_x:
                    dataset[start:, min_y:max_y, min_x] = data
                elif max_x < dataset.shape[2]:
                    dataset[start:, min_y:max_y, min_x:max_x] = data
                else: # max_x >= dataset.shape[2]
                    dataset[start:, min_y:max_y, min_x:] = data
            else: # max_y >= dataset.shape[1]
                if max_x == min_x:
                    dataset[start:, min_y:, min_x] = data
                elif max_x < dataset.shape[2]:
                    dataset[start:, min_y:, min_x:max_x] = data
                else: # max_x >= dataset.shape[2]
                    dataset[start:, min_y:, min_x:] = data


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class Hdf5DateGridFileReader(Hdf5DateGridReaderMixin, Hdf5GridFileReader):
    """ Provides read-only access to 3D gridded data in HDF5 files where
    the first dimension is time, the 2nd dimension is rows and the 3rd
    dimension is columns. Grids can contain any type of data. Indexing
    based on Time/Latitude/Longitude is available. Time indexes may be
    derived from date/time with earliest date at index 0. Row indexes
    may be derived from Latitude coordinates with minimum Latitude at
    row index 0. Columns indexes may be derived from Longitude
    coordinates with minimum Longitude at column index 0.

    Inherits all of the capabilities of Hdf5GridFileReader
    """

    def __init__(self, hdf5_filepath):
        Hdf5GridFileReader.__init__(self, hdf5_filepath)

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _loadManagerAttributes_(self):
        Hdf5GridFileReader._loadManagerAttributes_(self)
        self._loadDateGridAttributes_()


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class Hdf5DateGridFileManager(Hdf5DateGridManagerMixin, Hdf5GridFileManager):
    """ Provides read/write access to 3D gridded data in HDF5 files where
    the first dimension is time, the 2nd dimension is rows and the 3rd
    dimension is columns. Grids can contain any type of data. Indexing
    based on Time/Latitude/Longitude is available. Time indexes may be
    derived from date/time with earliest date at index 0. Row indexes
    may be derived from Latitude coordinates with minimum Latitude at
    row index 0. Columns indexes may be derived from Longitude
    coordinates with minimum Longitude at column index 0.

    Inherits all of the capabilities of Hdf5GridFileManager
    """

    def __init__(self, hdf5_filepath, mode='r'):
        Hdf5GridFileManager.__init__(self, hdf5_filepath, mode)

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _loadManagerAttributes_(self):
        Hdf5GridFileManager._loadManagerAttributes_(self)
        self._loadDateGridAttributes_()


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class Hdf5DateGridFileBuilder(Hdf5DateGridManagerMixin, Hdf5GridFileBuilder):
    """ Creates a new HDF5 file with read/write access to 3D gridded data
    where the first dimension is time, the 2nd dimension is rows and the
    3rd dimension is columns.

    Inherits all of the capabilities of Hdf5GridFileBuilder
    """

    def __init__(self, hdf5_filepath, start_date, end_date, lons, lats,
                       mode='w'):
        self.preInitDates(start_date, end_date)
        Hdf5GridFileBuilder.__init__(self, hdf5_filepath, lons, lats, mode)
        # set the time span for this file
        self.setFileAttributes(start_date=dateAsString(start_date),
                               end_date=dateAsString(end_date))
        # close the file to make sure attributes are saved
        self.close()
        # reopen the file in append mode
        self.open(mode='a')

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def preInitDates(self, start_date, end_date):
        self.start_date = asDatetimeDate(start_date)
        self.end_date = asDatetimeDate(end_date)

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _loadManagerAttributes_(self):
        Hdf5GridFileBuilder._loadManagerAttributes_(self)
        self._loadDateGridAttributes_()

