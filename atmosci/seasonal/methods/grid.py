
import numpy as N

#from atmosci.utils.units import convertUnits
from atmosci.utils.string import isFloat, isInteger, tupleFromString
from atmosci.units import convertUnits

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def isNanString(_string_):
    lower = _string_.lower()
    return lower == 'nan' or lower.endswith('.nan')

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def hdf5ReaderPatch(self):
    self.data = self.getData
    self.dataWhere = self.getDataWhere

    self.dataset = self.getDataset
    self.datasetAttribute = self.getDatasetAttribute
    self.datasetAttributes = self.getDatasetAttributes
    self.datasetShape = self.getDatasetShape
    self.datasetType = self.getDatasetType

    self.fileAttribute = self.getFileAttribute
    self.fileAttributes = self.getFileAttributes
    self.fileHierarchy = self.getFileHierarchy

    self.groupAttribute = self.getGroupAttribute
    self.groupAttributes = self.getGroupAttributes
    self.groupHierarchy = self.getGroupHierarchy

    self.object = self.getObject
    self.objectAttribute = self.getObjectAttribute
    self.objectAttributes = self.getObjectAttributes
    self.objectShape = self.getObjectShape


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class GridFileReaderMethods:

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # data packing parameters
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _getPackParams(self, dataset):
        packed_dtype = dataset.dtype
        packed_missing = dataset.attrs.get('missing', None)
        if packed_missing and isinstance(packed_missing, basestring):
            if isFloat(packed_missing): packed_missing = float(packed_missing)
            elif isInteger(packed_missing): packed_missing = int(packed_missing)
            elif isNanString(packed_missing): packed_missing = N.nan
        multiplier = dataset.attrs.get('multiplier', None)
        if multiplier and isinstance(multuplier, basestring):
            if isFloat(multiplier): multiplier = float(multiplier)
            elif isInteger(multiplier): multiplier = int(multiplier)
        params = [ packed_dtype, packed_missing, multiplier ]

        unpack = dataset.attrs.get('unpack', None)
        if unpack is None:
            params.extend([packed_dtype, packed_missing])
        else:
            if isinstance(unpack, basestring):
                unpack = tupleFromString(unpack)
            if len(unpack) == 1:
                data_missing = packed_missing
            else:
                data_missing = unpack[1]
                if isFloat(data_missing): data_missing = float(data_missing)
                elif isInteger(data_missing): data_missing = int(data_missing)
                elif isNanString(data_missing): data_missing = N.nan
            data_dtype = N.dtype(unpack[0])
            params.extend([data_dtype, data_missing])
        return tuple(params)


    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #
    # processing extracted data
    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _getUnpacker(self, dataset_name):
        return self._unpackers.get(dataset_name, None)

    def _postUnpack(self, dataset_path, data, **kwargs):
        to_units = kwargs.get('units', None)
        if to_units is not None:
            from_units = self.getDatasetUnits(dataset_path)
            if from_units is not None:
                data = convertUnits(data, from_units, to_units)
        return data

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _processDataOut(self, dataset_name, data, **kwargs):
        if kwargs.get('raw', False): return data
        data = self._unpackData(dataset_name, data, **kwargs)
        return self._postUnpack(dataset_name, data, **kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _unpackData(self, dataset_path, packed_data, **kwargs):
        # look for dataset-specific unpacker
        unpack = self._getUnpacker(dataset_path)
        if unpack is not None: return unpack(packed_data)

        # discover packing parameters for dataset, if they exist
        packed_dtype, packed_missing, multiplier, data_dtype, data_missing =\
        self._getPackParams(self.getDataset(dataset_path))

        if 'dtype' in kwargs: data_dtype = kwargs['dtype']
        if 'missing' in kwargs: data_missing = kwargs['missing']

        # handle NumPy arrays
        if isinstance(packed_data, N.ndarray):
            indexes = None
            # if packed_missing is different than data_missing
            # find indexes where packed data is missing
            if packed_missing is not None:
                if packed_missing is N.nan:
                    if data_missing is not N.nan:
                        indexes = N.where(N.isnan(packed_data))
                elif data_missing != packed_missing:
                    indexes = N.where(packed_data == packed_missing)
            # apply inverse of data multiplier if there is one
            if multiplier: data = packed_data * (1./multiplier)
            else: data = packed_data
            # convert data to correct dtype
            if data_dtype != packed_dtype:
                data = data.astype(data_dtype)
            # put missing value into array as necessary
            if indexes: data[indexes] = data_missing
            return data
        # handle single Python data values
        else:
            if packed_missing is not None:
                if (packed_missing is N.nan and packed_data is N.nan) \
                or packed_data == packed_missing:
                    return data_missing
            # apply inverse of data multiplier if there is one
            if multiplier: data = packed_data * (1./multiplier)
            else: data = packed_data
            # convert data to correct dtype
            if data_dtype != packed_dtype: data = data_dtype.type(data)
            return data


    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _whereMissing(self, data, **kwargs):
        missing = kwargs.get('missing', None)
        if missing is None:
            dataset = kwargs.get('dataset', None)
            if isinstance(dataset, basestring):
                dataset = self.getDataset(dataset)
            if dataset is not None:
                # discover packing parameters for dataset, if they exist
                pd, pm, m, dt, missing = self._getPackParams(dataset)

        if missing is None:
            # make a guess of missing value based of dtype of data:
            if data.dtype.kind == 'f':
                return N.nan, N.where(N.isnan(data))
            elif data.dtype.kind == 'i':
                # seasonal package default missing integer value
                missing = -32768
                indexes = N.where(data == missing)
                if len(indexes[0] == 0):
                    # try NOAA's default missing integer value
                    missing = -999
                    indexes = N.where(data == missing)
                return missing, indexes
        elif N.isnan(missing):
            return N.nan, N.where(N.isnan(data))
        else: return missing, N.where(data == missing)

        errmsg = 'Unable to determine missing value for data of type %s'
        raise ValueError, errmsg % str(data.dtype)


    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _getRegisteredFunction(self, key):
        return self.registry.functions.get(key, None)


    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #


    def _configDatasets_(self):
        pass

    def _loadDatasetAttrs_(self):
        pass

    def _loadProjectFileAttributes_(self):
        attrs = self.getFileAttributes()
        for attr_name, attr_value in attrs.items():
            if attr_name not in ('start_date', 'end_date', 'target_year'):
                self.__dict__[attr_name] = attr_value

        self._configDatasets_()
        self._loadDatasetAttrs_()
        self._postLoadFileAttrs_()

    def _preInitProject_(self, registry=None, **kwargs):
        if registry is None:
            from atmosci.utils.config import ConfigObject
            self.registry = ConfigObject('registry', None)
        else:
            self.registry = registry.copy()
        self._dataset_names = [ ]
        self._datasets_ = { }
        self._unpackers = { }

    def _postInitProject_(self, **kwargs):
        pass

    def _postLoadFileAttrs_(self):
        pass


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class GridFileManagerMethods:

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _setDefaultSearchRadius_(self):
        if hasattr(self, 'search_radius'):
            radius = self.search_radius
        elif self.lats is not None:
            lat_diff = N.nanmax(self.lats[1:,:] - self.lats[:-1,:])
            lon_diff = N.nanmax(self.lons[:,1:] - self.lons[:,:-1])
            # by default, use 55% of distance b/w farthest possible nodes
            # in a single grid rectangle
            radius = N.sqrt(lat_diff**2. + lon_diff**2.) * 0.55
        else: radius = None
        self.node_search_radius = radius


    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #
    # processing inserted data
    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _getPacker(self, dataset_path):
        return self._packers.get(dataset_path, None)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _packData(self, dataset_path, data, **kwargs):
        # look for dataset-specific input data packer
        packer = self._getPacker(dataset_path)
        if packer is not None: return packer(data)

        # discover packing parameters for dataset, if they exist
        # NOTE: ignore_datatype is what the file thinks the OUTPUT
        #       data type should be
        packed_dtype, packed_missing, multiplier, ignore_dtype, data_missing =\
        self._getPackParams(self.getDataset(dataset_path))

        # handle NumPy arrays
        if isinstance(data, N.ndarray):
            indexes = None
            if data_missing is not None:
                # if data_missing is different than packed_missing
                # find indexes where data is missing
                if data_missing is N.nan:
                    if packed_missing is not N.nan:
                        indexes = N.where(N.isnan(data))
                elif data_missing != packed_missing:
                    indexes = N.where(data == data_missing)
            # apply inverse of data multiplier if there is one
            if multiplier: packed_data = data * (1./multiplier)
            else: packed_data = data
            # convert data to dtype used in the file
            #
            # packing needs the actual dtype of the input data
            if data.dtype != packed_dtype:
                packed_data = packed_data.astype(packed_dtype)
            # put missing value into array as necessary
            if indexes: packed_data[indexes] = packed_missing
            return packed_data
        # handle single Python data values
        else:
            if data_missing is not None:
                if (data_missing is N.nan and data is N.nan) \
                or data == data_missing:
                    return packed_missing
            # apply data multiplier if there is one
            if multiplier: packed_data = data * multiplier
            else: packed_data = data
            if data_dtype != packed_dtype:
                # convert data to correct dtype
               packed_data = packed_dtype.type(packed_data)
            return packed_data

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _processDataIn(self, dataset_path, data, **kwargs):
        raw = kwargs.get('raw', False)
        if raw: return data
        data = self._prePack(dataset_path, data, **kwargs)
        return self._packData(dataset_path, data, **kwargs)

