""" Classes for accessing data from Hdf5 encoded grid files.
"""

import os
import math

import numpy as N

from atmosci.hdf5.file import Hdf5FileReader, Hdf5FileManager

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class Hdf5GridFileMixin:

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def getData(self, dataset_name, bounded=False, **kwargs):
        self.assertFileOpen()
        data = self._getData_(self.file, dataset_name, **kwargs)
        if kwargs.get('raw',False): return data
        return self._processDataOut(dataset_name, data, **kwargs)

    def getDataInBounds(self, dataset_name, **kwargs):
        self.assertFileOpen()
        dataset = self.getDataset(dataset_name)
        data = self._coordBoundsSubset(dataset)
        if kwargs.get('raw',False): return data
        return self._processDataOut(dataset_name, data, **kwargs)

    def get2DSlice(self, dataset_name, min_lon, max_lon, min_lat, max_lat,
                         **kwargs):
        min_y, min_x = self.ll2index(min_lon, min_lat)
        max_y, max_x = self.ll2index(max_lon, max_lat)
        dataset = self.getDataset(dataset_name)
        data = self._slice2DDataset(dataset, min_y, max_y, min_x, max_x)
        if kwargs.get('raw',False): return data
        return self._processDataOut(dataset_name, data, **kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def distanceBetweenNodes(self, lon1, lat1, lon2, lat2):
        lon_diffs = lon1 - lon2
        lat_diffs = lat1 - lat2
        return N.sqrt( (lon_diffs * lon_diffs) + (lat_diffs * lat_diffs) )

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def getBoundingBox(self):
        if self._bounding_box is None:
            return (self._min_avail_lon, self._min_avail_lat,
                    self._max_avail_lon, self._max_avail_lat)
        else: self._bounding_box

    def getIndexBounds(self):
        """ Returns a tuple containg the minimum and maximum x and y indexes
        """
        if self._index_bounds is None:
            return (self._min_avail_y, self._max_avail_y,
                    self._min_avail_x, self._max_avail_x)
        else: return self._index_bounds

    def getCoordinateLimits(self):
        limits = { }
        limits['min_avail_lon'] = self._min_avail_lon
        limits['max_avail_lon'] = self._max_avail_lon
        limits['min_avail_lat'] = self._min_avail_lat
        limits['max_avail_lat'] = self._max_avail_lat
        limits['max_avail_x'] = self._max_avail_x
        limits['min_avail_x'] = self._min_avail_x
        limits['max_avail_y'] = self._max_avail_y
        limits['min_avail_y'] = self._min_avail_y
        return limits

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    
    def getNodeData(self, dataset_name, lon, lat):
        y, x = self.ll2index(lon, lat)
        return self.getDataset(dataset_name)[y, x]

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def indexOfClosestNode(self, lon, lat):
        return self._indexOfClosestNode(lon, lat)

    def index2ll(self, y, x):
        """ Returns the lon/lat coordinates of grid node at the y/x index
        """
        return self._lons[y,x], self._lats[y,x]

    def ll2index(self, lon, lat):
        """ Returns the indexes of the grid node that is closest to the
        lon/lat coordinate point.
        """
        if hasattr(self, 'nodeIndexer'):
            return self.nodeIndexer(lon, lat)
        else: return self._indexOfClosestNode(lon, lat)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def setAreaMask(self, mask_name='mask'):
        """ Returns the area mask as an array.
        """
        mask_array, attrs = self.getRawData(mask_name)
        self._area_mask = mask_array

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def setCoordinateBounds(self, point_or_bbox, tolerance=None):
        if isinstance(point_or_bbox, basestring):
            point_or_bbox = eval(point_or_bbox)

        if len(point_or_bbox) == 2:
            self._coord_bounds = None
            self._y, self._x = self.ll2index(*point_or_bbox)
            self._index_bounds = None
        elif len(point_or_bbox) == 4:
            self._coord_bounds = tuple(point_or_bbox)
            lon, lat = point_or_bbox[:2]
            y1, x1 = self._indexOfClosestNode(lon, lat, tolerance)
            lon, lat = point_or_bbox[2:]
            y2, x2 = self._indexOfClosestNode(lon, lat, tolerance)
            self._index_bounds = (y1, x1, y2+1, x2+1)
            self._y = None
            self._x = None
        else:
            errmsg = "Invalid value for 'point_or_bbox'. It must contain "
            errmsg += "either a point (lon,lat) or a bounding box "
            errmsg += "(min_lon,min_lat,max_lon,max_lat)."
            raise ValueError, errmsg

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def setIndexBounds(self, point_or_bbox):
        if len(point_or_bbox) == 2:
            self._y = point_or_bbox[0]
            self._x = point_or_bbox[1]
            self._index_bounds = None
            self._coord_bounds = None
        elif len(point_or_bbox) == 4:
            min_y = point_or_bbox[0]
            min_x = point_or_bbox[1]
            max_y = point_or_bbox[2]
            max_x = point_or_bbox[3]
            self._index_bounds = (min_y, max_y, min_x, max_x)
            min_lon, min_lat = self.index2ll(*point_or_bbox[:2])
            max_lon, max_lat = self.index2ll(*point_or_bbox[2:])
            self._coord_bounds = (min_lon, min_lat, max_lon, max_lat)
            self._y = None
            self._x = None
        else:
            errmsg = "Invalid value for 'point_or_bbox'. It must contain "
            errmsg += "either a point (y,x) or a bounding box "
            errmsg += "(min_y,min_x,max_y,max_y)."
            raise ValueError, errmsg

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def setNodeSearchRadius(self, radius):
        self.node_search_radius = radius

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def unsetGridBounds(self):
        self._index_bounds = None
        self._coord_bounds = None
        self._y = None
        self._x = None

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _coordBoundsSubset(self, dataset):
        """ Returns a subset of a grid that is within the grid manager's
        lon/lat bounding box. Grid shape must be [y, x].
        """
        if self._index_bounds is not None:
            min_y, min_x, max_y, max_x = self._index_bounds

            if max_y < dataset.shape[0]:
                if max_x < dataset.shape[1]:
                    return dataset[min_y:max_y, min_x:max_x]
                else:
                    return dataset[min_y:max_y, min_x:]
            else:
                if max_x < dataset.shape[1]:
                    return dataset[min_y:, min_x:max_x]
                else:
                    return dataset[min_y:, min_x:]

        # asking for a single point
        elif self._x is not None:
            return  dataset[self._y, self._x]

        # asking for the whole dataset
        return dataset


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _indexOfClosestNode(self, target_lon, target_lat, tolerance=None):
        # "closeness" is dependent on the projection and grid spacing of
        # the data ... this implementation is decent for grids in the
        # continental U.S. with small node spacing (~ 5km or less) such
        # as the ACIS 5 km Lambert Conformal grids supplied by NRCC.
        # It should really be implemented in a subclass using a method
        # that is specific to the grid type in use.

        # try for anexact fit
        indexes = N.where( (self.lons == target_lon) &
                           (self.lats == target_lat) )
        if len(indexes[0]) > 0:
            return indexes[0][0], indexes[1][0]

        # search using user-requested tolerance
        if tolerance is not None:
            # first try to find a uique node within the tolerance
            indexes = N.where( (self.lons >= (target_lon - tolerance)) &
                               (self.lons <= (target_lon + tolerance)) )
            unique_lons = N.unique(self.lons[indexes])

            indexes = N.where( (self.lats >= (target_lat - tolerance)) &
                               (self.lats <= (target_lat + tolerance)) )
            unique_lats = N.unique(self.lats[indexes])
            if len(unique_lons) == 1 and len(unique_lats) == 1:
                indexes = N.where( (self.lons == unique_lons[0]) &
                                   (self.lats == unique_lats[0]) )
                return indexes[0][0], indexes[1][0]

            # no unique node witin tolerance, find any within tolerance 
            indexes = N.where( (self.lons >= (target_lon - tolerance)) &
                               (self.lons <= (target_lon + tolerance)) &
                               (self.lats >= (target_lat - tolerance)) &
                               (self.lats <= (target_lat + tolerance)) )
            
            if len(indexes[0]) == 1 and len(indexes[1]) == 1:
                return indexes[0][0], indexes[1][0]

        # search using radius specified in the file attributes
        else:
            radius = self.node_search_radius
            indexes = N.where( (self.lons >= (target_lon - radius)) &
                               (self.lons <= (target_lon + radius)) &
                               (self.lats >= (target_lat - radius)) &
                               (self.lats <= (target_lat + radius)) )
        # find node that is least distance from target coordinates
        distances = \
        self.distanceBetweenNodes(self.lons[indexes], self.lats[indexes],
                                  target_lon, target_lat)
        indx = N.where(distances == distances.min())[0][0]
        return indexes[0][indx], indexes[1][indx]


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _initCoordinateLimits(self):
        """ Sets the absolute limts for lon/lat and index coordinates for
        grids present in the file.
        """
        # initialize grid coordinate and index bounds to None
        self._lon_lat_bbox = None
        self._index_bounds = None

        # get lon/lat grids and capture grid limits
        try:
            if self.hasDataset('lon'):
                lat_ds_name = 'lat'
                lon_ds_name = 'lon'
            elif self.hasDataset('lons'):
                lat_ds_name = 'lats'
                lon_ds_name = 'lons'
            self.lats = self.getData(lat_ds_name, raw=True)
            self.lons = self.getData(lon_ds_name, raw=True)
        except:
            self._min_avail_lat = None
            self._max_avail_lat = None
            self._min_avail_lon = None
            self._max_avail_lon = None

            self._min_avail_x = None
            self._max_avail_x = None 
            self._min_avail_y = None
            self._max_avail_y = None

            self.lats = None
            self.lons = None
        else:
            # capture lat/lon limits
            self._min_avail_lat = N.nanmin(self.lats)
            self._max_avail_lat = N.nanmax(self.lats)
            self._min_avail_lon = N.nanmin(self.lons)
            self._max_avail_lon = N.nanmax(self.lons)

            # capture index limits
            self._min_avail_x = 0
            self._max_avail_x = self.lons.shape[1] - 1
            self._min_avail_y = 0
            self._max_avail_y = self.lons.shape[0] - 1

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _slice2DDataset(self, dataset, min_y, max_y, min_x, max_x):
        max_avail_x = dataset.shape[1] - 1
        max_avail_y = dataset.shape[0] - 1

        if max_x < max_avail_x : # max_x < dataset.shape[1]
            if max_y < max_avail_y : # max_y < dataset.shape[0]
                if max_x == min_x :
                    if max_y == min_y : # retrieve data for one node
                        return dataset[min_y, min_x]
                    # retrieve data for all y indexes in range
                    return dataset[min_y:max_y+1, min_x]
                else:
                    if max_y == min_y :
                        # retrieve data for x indexes in range
                        return dataset[min_y, min_x:max_x+1]
                    # retrieve data for all x & y indexes in range
                    return dataset[min_y:max_y+1, min_x:max_x+1]
            else: # max_y >= dataset.shape[0]
                # retrieve y indexes to end of shape[0]
                if max_x == min_x : # single x index
                    return dataset[min_y:, min_x]
                else: # x indexes in range, y indexes to end of shape[0]
                    return dataset[min_y:, min_x:max_x+1]

        else: # max_x >= dataset.shape[1]
            if max_y < max_avail_y : # max_y < dataset.shape[0]
                # retrieve x indexes to end of dataset.shape[1]
                if max_y == min_y : # single y coordinate
                    return dataset[min_y, min_x:]
                # retrieve data for y indexes in range, x indexes to end
                return dataset[min_y:max_y+1, min_x:]
            else: # retrieve x and y indexes to end of dataset
                return dataset[min_y:, min_x:]

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _clearManagerAttributes_(self):
        Hdf5DataFileManager._clearManagerAttributes_(self)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _loadGridFileAttributes_(self):
        Hdf5FileReader._loadManagerAttributes_(self)
        self.unsetGridBounds()
        self._initCoordinateLimits()
        self._loadGridExtentAttributes_()

    def _loadGridExtentAttributes_(self):
        if self.lats is not None:
            self.grid_shape = self.lats.shape
            self.grid_size = self.lats.size
        else:
            self.grid_shape = ()
            self.grid_size = -32768

        if not hasattr(self, 'node_search_radius') \
        or self.node_search_radius is None:
            self._setDefaultSearchRadius_()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _setDefaultSearchRadius_(self):
        if self.lats is not None:
            lat_diff = N.nanmax(self.lats[1:,:] - self.lats[:-1,:])
            lon_diff = N.nanmax(self.lons[:,1:] - self.lons[:,:-1])
            # 55% of distance between farthest nodes in a single grid rectangle
            self.node_search_radius = N.sqrt(lat_diff**2. + lon_diff**2.) * 0.55
        else: self.node_search_radius = None

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class Hdf5GridFileReader(Hdf5FileReader, Hdf5GridFileMixin):
    """ Provides read-only access to datasets, groups and other obsects in
    Hdf5-encoded files. Additinally provides special methods for accessing
    data in datasets based on regular Latitude/Longitude grids. Indexing
    based on Latitude/Longitude is available provided that rows are
    ordered by Latitude and columns are ordered by Longitude. For this to
    work, the minimum Latitude must be at row index 0 and the minimum
    Longitude must be at column index 0.

    Inherits all of the capabilities of Hdf5FileReader
    """

    def __init__(self, hdf5_filepath):
        Hdf5FileReader.__init__(self, hdf5_filepath)
        self._area_mask = None

    def _loadManagerAttributes_(self):
        Hdf5FileReader._loadManagerAttributes_(self)
        self._loadGridFileAttributes_()


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class Hdf5GridFileManager(Hdf5FileManager, Hdf5GridFileMixin):
    """ Provides read/write access to datasets, groups and other obsects in
    Hdf5-encoded files. Additinally provides special methods for accessing
    data in datasets based on regular Latitude/Longitude grids. Indexing
    based on Latitude/Longitude is available provided that rows are
    ordered by Latitude and columns are ordered by Longitude. For this to
    work, the minimum Latitude must be at row index 0 and the minimum
    Longitude must be at column index 0.

    Inherits all of the capabilities of Hdf5FileManager
    """

    def __init__(self, hdf5_filepath, mode='r'):
        Hdf5FileManager.__init__(self, hdf5_filepath, mode)
        self._area_mask = None

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def insertDataInBounds(self, dataset_name, data, **kwargs):
        data = self._processDataIn(dataset_name, data, **kwargs)
        dataset = self.getDataset(dataset_name)
        self._insertDataInBounds(dataset_name, dataset, data,)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def insert2DSlice(self, dataset_name, data, min_lon, max_lon, min_lat,
                            max_lat, **kwargs):
        data = self._processDataIn(dataset_name, data, **kwargs)
        min_y, min_x = self.ll2index(min_lon, min_lat)
        max_y, max_x = self.ll2index(max_lon, max_lat)
        dataset = self.getDataset(dataset_name)
        self._insert2DSlice(dataset, data, min_y, max_y, min_x, max_x)

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _insertDataInBounds(dataset_name, dataset, data):
        if self._index_bounds is not None:
            min_y, max_y, min_x, max_x = self._index_bounds

            if max_y < dataset.shape[0]:
                if max_x < dataset.shape[1]:
                    dataset[min_y:max_y, min_x:max_x] = data
                else: dataset[min_y:max_y, min_x:] = data
            else:
                if max_x < dataset.shape[1]:
                    dataset[min_y:, min_x:max_x] = data
                else: dataset[min_y:, min_x:] = data
        else:
            errmsg = 'Coordinate bounding box has not been defined.'
            raise AttributeError, errmsg

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _insert2DSlice(dataset, data, min_y, max_y, min_x, max_x):
        if max_y == min_y:
            if max_x == min_x: # retrieve data for one node
                dataset[min_y, min_x] = data
            elif max_x < dataset.shape[1]:
                dataset[min_y, min_x:max_x] = data
            else: # max_x >= dataset.shape[1]
                dataset[min_y, min_x:] = data
        elif max_y < dataset.shape[0]:
            if max_x == min_x:
                dataset[min_y:max_y, min_x] = data
            elif max_x < dataset.shape[1]:
                dataset[min_y:max_y, min_x:max_x] = data
            else: # max_x >= dataset.shape[1]
                dataset[min_y:max_y, min_x:] = data
        else: # max_y >= dataset.shape[0]
            if max_x == min_x:
                dataset[min_y:, min_x] = data
            elif max_x < dataset.shape[1]:
                dataset[min_y:, min_x:max_x] = data
            else: # max_x >= dataset.shape[1]
                dataset[min_y:, min_x:] = data

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _loadManagerAttributes_(self):
        Hdf5FileManager._loadManagerAttributes_(self)
        self._loadGridFileAttributes_()


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class Hdf5GridFileBuilderMixin:
    """ provides additional methods used for building grid files.
    """

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def initLonLatData(self, lons, lats, **kwargs):
        min_lon = N.nanmin(lons)
        max_lon = N.nanmax(lons)
        min_lat = N.nanmin(lats)
        max_lat = N.nanmax(lats)

        self.open(mode='a')
        # capture latitude grid limits
        self.setFileAttributes(min_lon=min_lon, max_lon=max_lon,
                               min_lat=min_lat, max_lat=max_lat)
        # close the file to make sure attributes are saved
        self.close()

        self.open(mode='a')
        if not self.hasDataset('lon'):
            self.createDataset('lon', lons, min=min_lon, max=max_lon, **kwargs)
        else:
            self.updateDataset('lon', lons)
            self.setDatasetAttributes('lon', min=min_lon, max=max_lon, **kwargs)
        # close the file to make sure everything are saved
        self.close()

        self.open(mode='a')
        if not self.hasDataset('lat'):
            self.createDataset('lat', lats, min=min_lat, max=max_lat, **kwargs)
        else:
            self.updateDataset('lat', lats)
            self.setDatasetAttributes('lat', min=min_lat, max=max_lat, **kwargs)
        # close the file to make sure everything are saved
        self.close()


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class Hdf5GridFileBuilder(Hdf5GridFileManager, Hdf5GridFileBuilderMixin):
    """ Creates a new HDF5 file with read/write access to datasets, groups,
    and other objects. 

    Inherits all of the capabilities of Hdf5GridFileManager
    """

    def __init__(self, hdf5_filepath, lons, lats, mode='w'):
        self._access_authority = ('r','a', 'w')
        Hdf5GridFileManager.__init__(self, hdf5_filepath, mode)
        self.setFileAttribute('created', self.timestamp)

        # create and initialize coordinate datasets
        if lons is not None and lats is not None:
            self.initLonLatData(lons, lats)
        
        # reopen the file in append mode
        self.open(mode='a')

