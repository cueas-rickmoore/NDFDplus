""" Classes for accessing data from Hdf5 encoded grid files.
"""

import os
from datetime import datetime

import h5py
import numpy as N

from atmosci.utils.data import safestring, safevalue, safedict
from atmosci.utils.data import safeDataKey, dictToWhere, listToWhere

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

DATASET_CREATE_ARGS = ('compression', 'compression_opts', 'chunks', 'dtype',
                       'fillvalue', 'fletcher32', 'maxshape', 'scaleoffset',
                       'shuffle')
TIME_SPAN_ERROR = 'Invalid time span. '
TIME_SPAN_ERROR += 'Either "date" OR "start_date" plus "end_date" are required.'

WALK_ERRMSG = "%s has no child named '%s'"

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class BogusValue:
    def __str__(self): return '!^BOGUS.VALUE^!'
BOGUS_VALUE = BogusValue()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def fullObjectPath(hdf5_object):
    path = [hdf5_object.name, ]
    parent = hdf5_object.parent
    while parent.name != '/':
        path.append(parent.name)
        parent = parent.parent
    if len(path) > 1:
        path.reverse()
        return '.'.join(path)
    elif len(path) == 1:
        return path[0]
    else:
        return 'file'

def walkForKeys(hdf5_object, include_datasets=True, include_groups=True):
    keys = [ ]
    for obj in hdf5_object.values():
        if isinstance(obj, h5py.Dataset):
            if include_datasets: keys.append(dottedKey(obj.name))
        else:
            if include_groups: keys.append(dottedKey(obj.name))
            keys.extend(walkForKeys(obj, include_datasets, include_groups))
    keys.sort()
    return tuple(keys)

def walkToObject(root_object, object_key):
    if isinstance(object_key, basestring):
        path = [safeDataKey(key) for key in object_key.split('.')]
    else: path = [safeDataKey(key) for key in object_key]
    try:
        _object = root_object[path[0]]
    except KeyError:
        raise KeyError, WALK_ERRMSG % (dottedPath(root_object),path[0])
    if len(path) == 1: return _object
    else: return walkToObject(_object, path[1:])

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class Hdf5DataReaderMixin:
    """ Mixin class that reads datasets, groups and other obsects in
    Hdf5-encoded files.
    """

    def assertDatasetObject(self, _object):
        if not isinstance(_object, h5py.Dataset):
            errmsg = "Object at '%s' is not an Hdf5 dataset."
            raise TypeError, errmsg % self.dotPath(_object)

    def assertFileObject(self, _object):
        if _object.name not in ('/',''):
            errmsg = "Object at '%s' is not an Hdf5 file."
            raise TypeError, errmsg % self.dotPath(_object)

    def assertGroupObject(self, _object):
        if isinstance(_object, h5py.Dataset):
            errmsg = "Object at '%s' is not an Hdf5 group."
            raise TypeError, errmsg % self.dotPath(_object)

    def hdfObjectKey(self, dot_path):
        return safeDataKey(dot_path).replace('.','/')


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # root-level data access
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _getData_(self, parent, dataset_name, **kwargs):
        dataset = self._getDataset_(parent, dataset_name)
        # index subset in kwargs
        if 'indexes' in kwargs:
            index_strings = [ ]
            for indx in kwargs['indexes']:
                if isinstance(indx, (tuple,list)):
                    index_strings.append(':'.join([str(it) for it in indx]))
                else: index_strings.append(str(indx))
            subset = ','.join(index_strings)
            return eval('dataset[%s]' % subset)
        # index to single element
        elif 'index' in kwargs:
            indx = int(kwargs['index'])
            dimensions = len(dataset.shape)
            if dimensions > 1:
                indexes = ','.join([':' for dim in range(dimensions-1)])
                return eval('dataset[%d,%s]' % (indx, indexes))
            else: return dataset[indx]
        # no indexes, return entire dataset
        else: return dataset[()]


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # root-level dataset access
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _getDataset_(self, parent, dataset_name):
        """ Returns the dataset indicated by dataset_key.
        """
        _object = self._getObject_(parent, dataset_name)
        self.assertDatasetObject(_object)
        return _object

    def _getDatasetAttribute_(self, parent, dataset_name, attr_name,
                                    default=BOGUS_VALUE):
        """ Returns a the value of a single attribute of the dataset
        indicated by dataset_key.
        """
        dataset = self._getDataset_(parent, dataset_name)
        try:
            return dataset.attrs[safeDataKey(attr_name)]
        except Exception as e:
            if default != BOGUS_VALUE: return default
            errmsg = "ERROR retrieving attribute '%s' from dataset '%s'"
            e.args = (errmsg % (attr_name, dataset_name),) + e.args
            raise e
    _getDatasetAttr_ = _getDatasetAttribute_

    def _getDatasetAttributes_(self, parent, dataset_name):
        """ Returns a dictionary containing all attributes of the dataset.
        """
        dataset = self._getDataset_(parent, dataset_name)
        return safedict(dict(dataset.attrs), safe_values=True)
    _getDatasetAttrs_ = _getDatasetAttributes_

    def _getDatasetKeys_(self, parent, dataset_name):
        """ Returns a tuple with the list of the keys for all datasets in
        the Hdf5 object.
        """
        dataset = self._getDataset_(parent, dataset_name)
        return self._getObjectKeys_(dataset)

    def _getDatasetShape_(self, parent, dataset_name):
        """ Returns a dictionary of attr_name/attr_value pairs for all
        setable attributes of the dataset .
        dataset = self._getDataset_(parent, dataset_name)
        """
        return self._getDataset_(parent, dataset_name).shape

    def _getDatasetType_(self, parent, dataset_name):
        """ Returns a dictionary of attr_name/attr_value pairs for all
        setable attributes of the dataset .
        """
        return self._getDataset_(parent, dataset_name).dtype


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # root-level file managemnt and attribute access
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _close_(self, data_file):
        """ Closes the file-like object passed by data_file.
        """
        self.assertFileObject(data_file)
        data_file.close()

    def _getFileAttribute_(self, data_file, attr_name, default=BOGUS_VALUE):
        self.assertFileObject(data_file)
        return self._getObjectAttribute_(data_file, attr_name, default)

    def _getFileAttributes_(self, data_file):
        self.assertFileObject(data_file)
        return self._getObjectAttributes_(data_file)

    def _openFile_(self, filepath, mode):
        """ Returns a pointer to an instance of a file-like object for
        accessing data in 'filepath'.
        """
        try:
            return h5py.File(filepath, mode)
        except:
            print 'Unable to open file in mode %s : %s' % (mode, filepath)
            raise


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # root-level group access
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _getGroup_(self, parent, group_name):
        """ Returns the dataset indicated by dataset_key.
        """
        _object = self._getObject_(parent, group_name)
        self.assertGroupObject(_object)
        return _object

    def _getGroupAttribute_(self, parent, group_name, attr_name,
                                  default=BOGUS_VALUE):
        group = self._getGroup_(parent, group_name)
        return self._getObjectAttribute_(group, attr_name, default)

    def _getGroupAttributes_(self, parent, group_name):
        group = self._getGroup_(parent, group_name)
        return self._getObjectAttributes_(group)

    def _getGroupKeys_(self, parent, group_name):
        """ Returns a tuple with the list of the keys for all datasets in
        the Hdf5 object.
        """
        group = self._getGroup_(parent, group_name)
        return self._getObjectKeys_(group)


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # root-level object-agnostic information access
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _getObject_(self, parent, object_name):
        """ Returns the object indicated by object_key.
        """
        object_key = self.hdfObjectKey(object_name)
        try:
            return parent[object_key]
        except KeyError as e:
            errmsg = "Hdf5 file does not have a data object named '%s'"
            e.args = (errmsg % object_key, self.filepath) + e.args
            raise e
        except Exception as e:
            errmsg = "Error during attempt to access data object named '%s'"
            e.args = (errmsg % object_key, self.filepath) + e.args
            raise e

    def _getObjectAttribute_(self, _object, attr_name, default=BOGUS_VALUE):
        """ Returns a the value of a single attribute of an object
        """
        try:
            return _object.attrs[safeDataKey(attr_name)]
        except Exception as e:
            if default != BOGUS_VALUE: return default
            errmsg = "ERROR retrieving attribute '%s' from object '%s'"
            e.args = (errmsg % (attr_name, _object.name),) + e.args
            raise e

    def _getObjectAttributes_(self, _object):
        """ Returns a dictionary of attr_name/attr_value pairs for all
        setable attributes of the object.
        """
        return safedict(dict(_object.attrs), safe_values=True)

    def _getObjectHierarchy_(self, _object, separator=None, grouped=False):
        if grouped:
            if _object.name != '/':
                if separator is not None:
                    groups = [_object.name[1:].replace('/', separator),]
                else: groups = [_object.name,]
            else: groups = [ ]
            datasets = [ ]

            for key in _object.keys():
                child = _object[key]
                if isinstance(child, h5py.Group):
                    _groups, _datasets =\
                    self._getObjectHierarchy_(child, separator, True)
                    if _groups: groups.extend(_groups)
                    if _datasets: datasets.extend(_datasets)
                else:
                    if separator is None: name = child.name
                    else: name = child.name[1:].replace('/', separator)
                    datasets.append(name)
            return groups, datasets
        else:
            if _object.name != '/':
                if separator is not None:
                    hierarchy = [_object.name[1:].replace('/', separator),]
                else: hierarchy = [_object.name,]
            else: hierarchy = [ ]

            for key in _object.keys():
                child = _object[key]
                if isinstance(child, h5py.Group):
                    _hierarchy = self._getObjectHierarchy_(child, separator)
                    if _hierarchy: hierarchy.extend(_hierarchy)
                else:
                    if separator is None: name = child.name
                    else: name = child.name[1:].replace('/', separator)
                    hierarchy.append(name)
            return hierarchy

    def _getObjectKeys_(self, _object):
        """ Returns a tuple with a list keys for all contained objects.
        """
        if hasattr(_object,'keys'):
            keys = _object.keys()
            keys.sort()
            return tuple(keys)
        else:
            errmsg = "%s object at '%s' does not support children"
            raise LookupError, errmsg % (_object.__class__.__name__,
                                         _object.name)

    def _getObjectShape_(self, _object):
        """ Returns a dictionary of attr_name/attr_value pairs for all
        setable attributes of the dataset .
        """
        if isinstance(_object, h5py.Dataset): return _object.shape()
        else: return len(_object.keys())

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _timestamp_(self, date_time=None):
        if date_time is None:
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        else:
            return date_time.strftime('%Y-%m-%d %H:%M:%S')
    timestamp = property(_timestamp_)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class Hdf5DataWriterMixin:
    """ Mixin class that allows creation of and updates to datasets, groups
    and other objects in Hdf5-encoded files.
    """

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # root-level dataset creation and change mangement
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _createDataset_(self, parent, dataset_name, numpy_array, **kwargs):
        """ Creates a new dataset in the data file and returns a pointer to
        it. Raises IOError exception if the dataset already exists.
        """
        dataset_key = self.hdfObjectKey(dataset_name)
        if dataset_key in parent.keys():
            errmsg = "'%s' dataset already exists in current data file."
            raise IOError, errmsg % dataset_name

        create_args = { }
        attributes = { }
        for name in kwargs:
            if name in DATASET_CREATE_ARGS:
                create_args[safestring(name)] = safevalue(kwargs[name])
            else: attributes[safestring(name)] = safevalue(kwargs[name])

        if 'created' not in attributes:
            attributes['created'] = self._timestamp_()

        dataset = parent.create_dataset(dataset_key, data=numpy_array,
                                        **create_args)

        for attr_name, attr_value in attributes.items():
            dataset.attrs[attr_name] = attr_value

        return dataset

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _createEmptyDataset_(self, parent, dataset_name, shape, dtype,
                                   **kwargs):
        """ Creates an empty dataset in the data file and returns a pointer
        to it. Raises IOError exception if the dataset already exists.
        """
        dataset_key = self.hdfObjectKey(dataset_name)
        if dataset_key in parent.keys():
            errmsg = "'%s' dataset already exists in current data file."
            raise IOError, errmsg % dataset_name

        create_args = { }
        attributes = { }
        for name in kwargs:
            if name in DATASET_CREATE_ARGS:
                create_args[safestring(name)] = safevalue(kwargs[name])
            else: attributes[safestring(name)] = safevalue(kwargs[name])

        if 'created' not in attributes:
            attributes['created'] = self._timestamp_()

        if dtype == N.dtype(object):
             create_args['dtype'] = h5py.new_vlen(str)
        else: create_args['dtype'] = dtype

        dataset = parent.create_dataset(dataset_key, shape, **create_args)

        for attr_name, attr_value in attributes.items():
            dataset.attrs[attr_name] = attr_value

        return dataset

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _updateDataset_(self, parent, dataset_name, numpy_array, attributes,
                        **kwargs):
        """ Update a dataset in the data file. If the dataset does not exist,
        it is created. Returns a pointer to the dataset.
        """
        array_dimensions = len(numpy_array.shape)
        dataset = self._getDataset_(parent, dataset_name)

        # replace the entire dataset
        if numpy_array.shape == dataset.shape:
            indexes = ','.join([':' for dim in range(array_dimensions)])
            update_string = 'dataset[%s] = numpy_array[%s]' % (indexes,indexes)
            exec(update_string)
            if attributes:
                self._setObjectAttributes_(dataset, attributes)
            return dataset

        # update a portion of the dataset
        # dimensions of new data must be compatible with dimensions of dataset
        dataset_dimensions = len(dataset.shape)
        if array_dimensions > dataset_dimensions:
            errmsg = \
                'Cannot insert array of shape %s into dataset of shape %s'
            raise IndexError, errmsg % (str(numpy_array.shape),
                                        str(dataset.shape))

        # new data goes into multi-dimensional slice
        if 'indexes' in kwargs:
            index_strings = [ ]
            for indx in kwargs['indexes']:
                if isinstance(indx, (tuple,list)):
                    index_strings.append(':'.join([str(it) for it in indx]))
                else: index_strings.append(str(indx))
            subset = ','.join(index_strings)
            update_string = 'dataset[%s] = numpy_array' % subset
            try:
                exec(update_string)
            except:
                errmsg = \
                    'Cannot insert %s array at [%s] in dataset of shape %s'
                raise IndexError, errmsg % (str(numpy_array.shape), subset,
                                            str(dataset.shape))

            if attributes:
                self._setObjectAttributes_(dataset, attributes)
            return dataset

        # new data replaces entire content slice at index in first dimension
        elif 'index' in kwargs:
            indx = kwargs['index']
            from_indexes = ','.join([':' for dim in range(array_dimensions)])
            from_array = 'numpy_array[%s]' % from_indexes
            if array_dimensions < dataset_dimensions:
                indexes = \
                    ','.join([':' for dim in range(dataset_dimensions-1)])
                to_array = 'dataset[%d,%s]' % (int(indx), indexes)
                update_string = 'dataset[%s] = numpy_array[%s]' % subset
                exec(update_string)
                if attributes:
                    self._setObjectAttributes_(dataset, attributes)
                return dataset
            else:
                errmsg = 'Cannot insert %s array into %s dataset as index %d.'
                raise IndexError, errmsg % (str(numpy_array.shape),
                                            str(dataset.shape), indx)

        # no indexes passed, can't do anything with mismatched dimensions
        else:
            errmsg = \
                'Not enough information to insert %s array into %s dataset.'
            raise IndexError, errmsg % (str(numpy_array.shape),
                                            str(dataset.shape))

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _deleteDatasetAttribute_(self, parent, dataset_name, attr_name):
        dataset = self._getDataset_(parent, dataset_name)
        self._deleteObjectAttribute_(dataset, attr_name)
 
    def _setDatasetAttribute_(self, parent, dataset_name, attr_name, attr_value):
        dataset = self._getDataset_(parent, dataset_name)
        self._setObjectAttribute_(dataset, attr_name, attr_value)

    def _setDatasetAttributes_(self, parent, dataset_name, attr_dict):
        dataset = self._getDataset_(parent, dataset_name)
        self._setObjectAttributes_(dataset, attr_dict)


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # root-level file attribute change mangement
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _deleteFileAttribute_(self, data_file, attr_name):
        self.assertFileObject(data_file)
        return self._deleteObjectAttribute_(data_file, attr_name)

    def _setFileAttribute_(self, data_file, attr_name, attr_value):
        self.assertFileObject(data_file)
        self._setObjectAttribute_(data_file, attr_name, attr_value)

    def _setFileAttributes_(self, data_file, attr_dict):
        self.assertFileObject(data_file)
        self._setObjectAttributes_(data_file, attr_dict)


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # root-level group creation and change mangement
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _createGroup_(self, parent, group_name, **kwargs):
        """ Creates a new group in the parent and returns a pointer to
        it. Raises IOError exception if the group already exists.
        """
        group_key = self.hdfObjectKey(group_name)
        if group_key in parent.keys():
            errmsg = "'%s' group already exists in %s"
            raise IOError, errmsg % (group_name, parent.name)

        group = parent.create_group(group_name)
        if kwargs:
            for attr_name, attr_value in kwargs.items():
                self._setObjectAttribute_(group, attr_name, attr_value)
        return group

    def _deleteGroupAttribute_(self, parent, group_name, attr_name):
        group = self._getGroup_(parent, group_name)
        return self._deleteObjectAttribute_(group, attr_name)

    def _setGroupAttribute_(self, parent, group_name, attr_name, attr_value):
        group = self._getGroup_(parent, group_name)
        self._setObjectAttribute_(group, attr_name, attr_value)

    def _setGroupAttributes_(self, parent, group_name, attr_dict):
        group = self._getGroup_(parent, group_name)
        self._setObjectAttributes_(group, attr_dict)


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # root-level object-agnotic creation and change mangement
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _deleteObject_(self, parent, object_name):
        """ deletes the object indicated by object_key.
        """
        object_key = self.hdfObjectKey(object_name)
        if object_key in parent.keys(): del parent[object_key]

    def _deleteObjectAttribute_(self, _object, attr_name):
        """ Deletes an attribute of an object.
        """
        try:
            del _object.attrs[safeDataKey(attr_name)]
        except Exception as e:
            errmsg = "Could not delete attribute '%s' of object '%s'"
            errmsg = errmsg % (attr_name, _object.name)
            e.args = (errmsg,) + e.args
            raise

    def _setObjectAttribute_(self, _object, attr_name, attr_value):
        """ Returns a dictionary of attr_name/attr_value pairs for all
        setable attributes of the dataset .
        """
        try:
            _object.attrs[safeDataKey(attr_name)] = safestring(attr_value)
        except Exception as e:
            errmsg = "Could not set attribute '%s' to '%s' for object '%s'"
            errmsg = errmsg % (attr_name, str(attr_value), _object.name)
            e.args = (errmsg,) + e.args
 
    def _setObjectAttributes_(self, _object, attr_dict):
        for attr_name, attr_value in attr_dict.items():
            self._setObjectAttribute_(_object, attr_name, attr_value)

