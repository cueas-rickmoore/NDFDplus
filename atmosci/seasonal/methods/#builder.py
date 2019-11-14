
import datetime
from copy import copy, deepcopy

import numpy as N

from atmosci.utils.timeutils import asDatetimeDate, asAcisQueryDate, ONE_DAY


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class FileBuilderMethods:
    """ 
    Provides common methods requred to build files containing grids
    with no time/date component.

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    #
    # REQUIRED - builder-specific initialization methods must be included
    #            in the __init__ method of any builder class
    #
    # preInitBuilder MUST be called by __init__ method of any builder
    # subclass prior to the superclass.__init__ method
    #
    # initFileAttributes MUST be called by __init__ method of any builder
    # subclass after the superclass.__init__ method
    #
    # postInitBuilder MUST be called by __init__ method after
    # initFileAttributes
    #
    # def __init__(self, project_config, filetype, source, ... , **kwargs):
    #
    #     self.preInitBuilder(project_config, filetype, source, **kwargs)
    #
    #     superclass.__init__(...)
    #
    #     self.initFileAttributes(**kwargs(
    #
    #     self.postInitBuilder(**kwargs)
    #
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    """

    def preInitBuilder(self, project_config, filetype, source, region=None,
                             **kwargs):
        self.debug = kwargs.get('debug',False)

        # initialize protected instance attributes
        self._access_authority = ('r','a', 'w')
    
        # initialize private instance attributes
        self.__config = project_config.copy()
        self.__filetype = self.__config.getFiletype(filetype)
        if isinstance(source, basestring):
            self.__source = self.__config.sources[source]
        else: self.__source = source
        self.source_tag = self.__source.get('tag', self.__source.name)

        if region is None:
            region_key = kwargs.get('region', self.__config.project.region)
            self.__region = self.__config.regions[region_key]
        elif isinstance(region, basestring):
            self.__region = self.__config.regions[region]
        else: self.__region = region

        self._additionalPreBuildInit(**kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def postInitBuilder(self, **kwargs):
        pass

    def _additionalPreBuildInit(self, **kwargs):
        pass

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # REQUIRED - initialize common file attributes
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def initFileAttributes(self, **kwargs):
        file_attrs =  self._resolveCommonAttributes(**kwargs)

        content = self.filetype.get('content', None)
        if content is not None: file_attrs['content'] = content

        scope = self.filetype.get('scope',None)
        if scope is not None: file_attrs['scope'] = scope

        file_attrs.update(self.sourceFileAttributes(**kwargs))

        # subclasses may require additional file attributes
        more_attrs = self.additionalFileAttributes(**kwargs)
        if more_attrs: file_attrs.update(more_attrs)

        self.open('a')
        self.setFileAttributes(**file_attrs)
        self.close()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def sourceFileAttributes(self, **kwargs):
        source = self.source
        attrs = { 'source': source.description, }
        node_spacing = source.get('node_spacing',None)
        if node_spacing is not None:
            attrs['node_spacing'] = node_spacing 
        search_radius = source.get('search_radius',None)
        if search_radius is not None:
            attrs['search_radius'] = search_radius

        bbox = kwargs.get('bbox', None)
        if bbox is not None: attrs['data_bbox'] = bbox

        description = self._fileDescription(source, **kwargs)
        attrs['description'] = description

        return attrs

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    # default is no additional file attributes
    def additionalFileAttributes(self, **kwargs):
        return { }


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # properties - access to protected and private attributes
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @property
    def config(self):
        return self.__config

    @property
    def filetype(self):
        return self.__filetype

    @property
    def project(self):
        return self.__config.get('project',None)

    @property
    def region(self):
        return self.__region

    @property
    def source(self):
        return self.__source


    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #
    # dataset build methods
    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def datasetAlreadyExists(self,  dataset_key, group_path=None):
        try:
            if group_path is not None:
                self._getDataset_(group_path, dataset_name)
            else:
                self._getDataset_(self.file, dataset_name)
        except:
            return False
        else:
            return True

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def buildDataset(self, dataset_key, group_path=None, group_keydict=None,
                           **kwargs):
        dataset_name, dataset, dataset_keydict = \
                               self._datasetConfig(dataset_key, **kwargs)
        self.open('a')
        if self.datasetAlreadyExists(dataset_key, group_path):
            self.close()
            return
        self.close()

        if group_path is not None:
            dataset_path = '%s.%s' % (group_path, dataset_name)
        else: dataset_path = dataset_name

        shape, dtype, attrs = \
        self._resolveDatasetBuildAttributes(dataset, dataset_keydict,
                                            group_keydict, **kwargs)
        if 'shape' in kwargs: del kwargs['shape']

        view = attrs.get('view',None)
        chunks = self._resolveDatasetChunks(dataset, shape, view, **kwargs)
        if chunks is not None:
            attrs['chunks'] = chunks
            if "compression" in dataset:
                attrs['compression'] = dataset.compression
            else: attrs['compression'] = 'gzip'

        data = kwargs.get('%s_data' % dataset_name, kwargs.get('data', None))
        if data is None:
            missing = attrs.get('missing', self.defaultMissingForType(dtype))
            data = N.full(shape, missing, dtype)

        if data.shape == shape:
            self.open('a')
            self.createDataset(dataset_path, data, **attrs)
            self.close()
        else:
            errmsg = 'Shape of input data %s does not match' % str(data.shape)
            errmsg = '%s shape of new dataset %s.' % (errmsg, str(shape))
            raise ValueError, errmsg

        return dataset_path

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def initGroupLonLat(self, group_path, lons, lats, **kwargs):
        """
        Add latitude and longitude datasets to group using data from
        "lons" and "lats" arguments.
        """
        shape = lons.shape
        # build the latitude dataset
        path = '%s.%s' % (group_path, 'lat')
        if not self.hasDataset(path):
            self.buildDataset('lat', group_path, shape=shape, **kwargs)

        min_lat = N.nanmin(lats)
        max_lat = N.nanmax(lats)
        self.open('a')
        dataset = self.getDataset('lat')
        dataset[:,:] = lats[:,:]
        dataset.attrs['max'] = max_lat
        dataset.attrs['min'] = min_lat
        self.close()

        # build the longitude dataset
        if not self.hasDataset('lon'):
            self.buildDataset('lon', group_path, shape=lons.shape, **kwargs)

        min_lon = N.nanmin(lons)
        max_lon = N.nanmax(lons)
        self.open('a')
        dataset = self.getDataset('lon')
        dataset[:,:] = lons[:,:]
        dataset.attrs['max'] = max_lon
        dataset.attrs['min'] = min_lon
        self.close()

        # capture longitude/latitude limits as file attributes
        self.open('a')
        self.setGroupAttributes(group_path, min_lon=min_lon, max_lon=max_lon,
                                min_lat=min_lat, max_lat=max_lat)
        self.close()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def initLonLatData(self, lons, lats, **kwargs):
        """
        Build the latitude and longitude datasets from information
        in their configuration records and the data passed in the
        "lons" and "lats" arguments.
        """
        # build the latitude dataset
        if not self.hasDataset('lat'):
            self.buildDataset('lat', shape=lats.shape, **kwargs)

        min_lat = N.nanmin(lats)
        max_lat = N.nanmax(lats)
        self.open('a')
        dataset = self.getDataset('lat')
        dataset[:,:] = lats[:,:]
        dataset.attrs['max'] = max_lat
        dataset.attrs['min'] = min_lat
        self.close()

        # build the longitude dataset
        if not self.hasDataset('lon'):
            self.buildDataset('lon', shape=lons.shape, **kwargs)

        min_lon = N.nanmin(lons)
        max_lon = N.nanmax(lons)
        self.open('a')
        dataset = self.getDataset('lon')
        dataset[:,:] = lons[:,:]
        dataset.attrs['max'] = max_lon
        dataset.attrs['min'] = min_lon
        self.close()

        # capture longitude/latitude limits as file attributes
        self.open('a')
        self.setFileAttributes(min_lon=min_lon, max_lon=max_lon,
                               min_lat=min_lat, max_lat=max_lat)
        self.close()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def initStaticDataset(self, dataset_key, data, track_stats, group=None, **kwargs):
        debug = kwargs.get('debug',False)

        # build the dataset
        dataset_path = self.buildDataset(dataset_key, group, **kwargs)
        if debug: print 'initStaticDataset', dataset_path

        if data is not None:
            self.open('a')
            dataset = self.getDataset(dataset_path)
            dataset[:,:] = data[:,:]
            if track_stats:
                dataset.attrs['max'] = N.nanmax(data)
                dataset.attrs['min'] = N.nanmin(data)
            self.close()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def defaultMissingForType(self, dtype):
        if isinstance(dtype, N.dtype):
            if dtype.kind == 'f': return N.nan      # floats
            elif dtype.kind == 'i': return -32768   # integers
            elif dtype.kind == 'b': return 0        # boolean
            elif dtype.kind in ('S','U'): return '' # strings
            elif dtype.kind == 'u': return 9999     # unsigned ints
        else:
            if dtype == float: return N.nan
            elif dtype == int: return -32768
            elif dtype == bool: return 0
            elif dtype in (str,unicode): return ''
        return None


    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #
    # config object access methods
    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _datasetConfig(self, dataset_key, **kwargs):
        print '\n\n', dataset_key
        print self.config.datasets.keys()
        print '\n\n'
        exit()
        if isinstance(dataset_key,(tuple,list)):
            key, keys = dataset_key
            if 'path' in keys: 
                name = keys['path']
                del keys['path']
                if len(keys) == 0: keys = None
            else: name = key
            dataset = self.config.datasets[key]
        elif isinstance(dataset_key,basestring):
            keys = None
            if ':' in dataset_key:
                name, key = dataset_key.split(':')
            else: name = key = dataset_key
            dataset = self.config.datasets[key]
            name = dataset.get('path', name)
        else:
            errmsg = 'Invalid type for dataset key : "%s"' 
            raise TypeError, errmsg % str(dataset_key)

        base = dataset.get('base',None)
        if base is not None:
            dataset.inheritAttrs(self.config.datasets[base])
        return name, dataset, keys

    _getDatasetConfig = _datasetConfig # backwards compatibility 

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _fileDescription(self, source, **kwargs):
        source_tag = source.get('tag', source.name.upper())
        template = self._fileDescriptionTemplate(self.filetype)
        descrip_dict = copy(kwargs)
        if 'source' not in descrip_dict: 
            descrip_dict['source'] = source_tag
        return template % descrip_dict

    _getFileDescription = _fileDescription # backwards compatibility

    def _fileDescriptionTemplate(self, filetype):
        if isinstance(filetype, basestring):
            filetype_cfg = self.config.filetypes[filetype]
        else: filetype_cfg = filetype
        template = filetype_cfg.get('description',
                   '%%(source)s %s' % filetype.name.title())
        return template

    # backwards compatibility
    _getFileDescriptionTemplate = _fileDescriptionTemplate

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _groupConfig(self, group_key):
        if isinstance(group_key,(tuple,list)):
            key = group_key[0] 
            key_dict = deepcopy(group_key[1])
            if 'path' in key_dict: 
                name = key_dict['path']
            else: name = key
            group = self.config.groups[key]
        elif isinstance(group_key,basestring):
            key_dict = None
            if ':' in group_key:
                name, key = group_key.split(':')
            else: name = key = group_key
            group = self.config.groups[key]
            name = group.get('path', name)
        else:
            errmsg = 'Invalid type for group key : "%s"' 
            raise TypeError, errmsg % str(group_key)

        return name, group, key_dict

    _getGroupConfig = _groupConfig # backwards compatibility

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _sourceDimensions(self):
        dimensions = self.source.get('grid_dimensions', None)
        if dimensions is not None:
            lat_dim = dimensions.get('lat', None)
            if lat_dim is not None: return dimensions

            region = dimensions.get(self.region.name, None)
            if region is not None: return region

        errmsg = \
            'Cannot determine dimensions of grid in %s source or %s region'
        raise KeyError, errmsg % (self.source.tag, self.region.name)

    _getSourceDimensions = _sourceDimensions 
    # backwards compatibility

    def _mappedDatasetView(self, dataset):
        return self.config.view_map.attrs[dataset.view]

    _getMappedView = _mappedDatasetView # backwards compatibility

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _loadProjectFileAttributes_(self, **kwargs):
        pass


    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #
    # attribute resolution methods
    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _packTypeStr(self, pack_type):
        if isinstance(pack_type, basestring):
            type_str = pack_type
        else: type_str = str(pack_type)

        quote = type_str.find("'")
        if quote >= 0:
            type_str = type_str[quote+1:]
            quote = type_str.find("'")
            type_str = type_str[:quote]
        else:
            quote = type_str.find('"')
            if quote >= 0:
                type_str = type_str[quote+1:]
                quote = type_str.find('"')
                type_str = type_str[:quote]
        return type_str

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _resolveCommonAttributes(self, **kwargs):
        attrs = { }
        created = kwargs.get('created', self.timestamp)
        if isinstance(created, (datetime.datetime, datetime.date)):
            created = self._timestamp_(created)
        attrs['created'] = created

        updated = kwargs.get('updated', 'never')
        if isinstance(updated, (datetime.datetime, datetime.date)):
            updated = self._timestamp_(updated)
        attrs['updated'] = updated

        source_tag = kwargs.get( 'source_tag', self.source.get('tag',
                                 self.source.name) )
        attrs['source'] = source_tag
        return attrs

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _resolveDatasetBuildAttributes(self, dataset_config, keydict=None,
                                             group_keydict=None, **kwargs):
        _keydict_ = copy(kwargs)

        if keydict is not None:
            for key, value in keydict.items():
                if key not in _keydict_: _keydict_[key] = value
        if group_keydict is not None:
            for key, value in group_keydict.items():
                if key not in _keydict_: _keydict_[key] = value

        attrs = self._resolveCommonAttributes(**kwargs)
        attrs['description'] = \
            self._resolveDatasetDescription(dataset_config, **_keydict_)
        attrs.update(self._resolveSourceAttributes(**kwargs))
        attrs.update(self._resolveScopeAttributes(dataset_config, **kwargs))
        time_attrs = self._resolveTimeAttributes(dataset_config, **kwargs)
        if time_attrs: attrs.update(time_attrs)
 
        shape = _keydict_.get('shape', None)
        if shape is None: 
            view, shape = self._shapeForDataset(dataset_config)
        else:
            del _keydict_['shape']
            view = dataset_config.get('view', None)

        if view is not None: attrs['view'] = view

        dtype = dataset_config.dtype
        packed_dtype = dataset_config.get('dtype_packed', dtype)

        missing = dataset_config.get('missing_data', None)
        packed_missing = dataset_config.get('missing_packed', None)
        if packed_missing is not None: attrs['missing'] = packed_missing
        elif missing is not None: attrs['missing'] = missing

        multiplier = dataset_config.get('multiplier', None)
        if multiplier: attrs['multiplier'] = multiplier

        units = dataset_config.get('units', None)
        if units is not None:
            if multiplier:
                attrs['units'] = '%s*%s' % (units, str(multiplier))
            else: attrs['units'] = units

        #unpack = (self._packTypeStr(dtype), str(missing))
        if dtype != packed_dtype:
            if dtype == float:
                if missing is not None:
                    if N.isnan(missing):
                        attrs['unpack'] = '(float,N.nan)'
                    else: attrs['unpack'] = '(float,%s)' % str(missing)
                else: attrs['unpack'] = '(float,None)'
            elif dtype == int:
                if missing is not None:
                    attrs['unpack'] = '(int,%s)' % str(missing)
                else: attrs['unpack'] = '(int,None)'
            elif dtype != bool:
                if missing is not None:
                    attrs['unpack'] = '(%s,%s)' % (dtype, str(missing))
                else: attrs['unpack'] = '(%s,None)' % dtype

        return shape, packed_dtype, attrs

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _resolveDatasetChunks(self, dataset, shape, view, **kwargs):
        chunks = kwargs.get('chunks',None)
        if chunks is not None: return chunks
        chunks = dataset.get('chunks', None)
        if chunks is not None: return chunks
        if len(shape) == 3:
            if view[0] == 't': return (1, shape[1], shape[2])
            elif view[2] == 't': return (1, 1, shape[2])
        return None

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _resolveDatasetDescription(self, dataset, **kwargs):
        return dataset.description % kwargs

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _resolveGroupBuildAttributes(self, group, group_keydict, **kwargs):
        attrs = self._resolveCommonAttributes(**kwargs)
        attrs.update(self._resolveSourceAttributes(**kwargs))
        if group_keydict is not None:
            keydict = copy(kwargs)
            for key, value in group_keydict.items():
                if key not in keydict: keydict[key] = value
            attrs['description'] = group.description % keydict
        else: attrs['description'] = group.description % kwargs
        return attrs

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _resolveScopeAttributes(self, dataset_config, **kwargs):
        attrs = { }
        scope = kwargs.get('scope', dataset_config.get('scope',None))
        if scope is None: return attrs
        attrs['scope'] = scope
        # is it a time series scope
        period = kwargs.get('period', dataset_config.get('period',None))
        if period is None: return attrs
        attrs['period'] = period
        return attrs
 
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _resolveSourceAttributes(self, **kwargs):
        source = self.source
        attrs = { 'source':kwargs.get('source_tag',
                                  source.get('tag', source.name)) }

        acis_grid = source.get('acis_grid', None)
        if acis_grid is not None:
            attrs['acis_grid'] = acis_grid
            acis = self.config.sources.acis
            node_spacing = acis.node_spacing
            search_radius = acis.search_radius
        else:
            node_spacing = source.get('node_spacing', None)
            search_radius = source.get('search_radius', None)

        if node_spacing is not None:
            attrs['node_spacing'] = node_spacing
        if search_radius is not None:
            attrs['node_search_radius'] = search_radius

        details = kwargs.get('source_detail', source.get('description', None))
        if details is not None: attrs['source_detail'] = details

        return attrs
 
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _resolveTimeAttributes(self, object_config, **kwargs):
        attrs = { }
        return attrs

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _shapeForDataset(self, dataset):
        errmsg = "Cannot unravel dimension '%s'"
        source_dimensions = self._sourceDimensions()
        view = self._mappedDatasetView(dataset)

        shape = [ ]
        for dim in dataset.view:
            if isinstance(dim, basestring):
                if dim in source_dimensions:
                    shape.append(source_dimensions[dim])
                else:
                    raise ValueError, errmsg % dim
            elif isinstance(dim, int):
                shape.append(dim)
            else:
                raise ValueError, errmsg % str(dim)
        return view, tuple(shape)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class GridFileBuildMethods(FileBuilderMethods):

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # build everything
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def build(self, build_groups=True, build_datasets=True, lons=None,
                    lats=None, **kwargs):

        # initiaize file attributes
        self.initFileAttributes(**kwargs)

        # build data groups
        groups = self.filetype.get('groups',None)
        if groups and build_groups:
            if self.debug: print 'building file level groups'
            for group_key in groups:
                group_name, group, group_keydict = self._groupConfig(group_key)
                if not self.groupExists(group_name):
                    self.open('a')
                    self.buildGroup(group_name, build_datasets, **kwargs)
                    self.close()

        # build file-level datasets
        datasets = self.filetype.get('datasets',None)
        # initialze lat and lon datasets if the data was passed
        if datasets and build_datasets:
            if lons is not None and lats is not None:
                self.initLonLatData(lons, lats)
                datasets = list(datasets)
                datasets.remove('lon')
                datasets.remove('lat')
        # test again, in case lats and lons were the only datasets in the list
        if datasets and build_datasets:
            if self.debug: print 'building file level datasets'
            for dataset_key in datasets:
                self.open('a')
                self.buildDataset(dataset_key, **kwargs)
                self.close()

        # return with file open for updates
        self.open('a')


    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #
    # data group initiailzation methods
    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def buildGroup(self, group_key, build_datasets, root_path=None, **kwargs):
        group_name, group, group_keydict = self._groupConfig(group_key)
        if self.groupExists(group_name, root_path): return
        
        if root_path is not None:
            group_path = '%s.%s' % (root_path, group_name)
        else: group_path = group_name
        if self.debug: print 'building %s group' % group_path

        attrs = \
            self._resolveGroupBuildAttributes(group, group_keydict, **kwargs)

        self.open('a')
        self.createGroup(group_path, **attrs)
        self.close()

        # build this group's sub-groups
        groups = group.get('groups', None)
        if groups != None:
            if self.debug: print 'building subgroups of', group_path
            for name in groups:
                self.buildGroup(name, build_datasets, group_path, **kwargs)

        if build_datasets:
            # build this group's datasets
            datasets = group.get('datasets', None)
            print '\n\ndatasets :'
            print datasets
            print '\n\n'
            if datasets is not None:
                if self.debug: print 'building datasets in group', group_path
                for dataset_key in datasets:
                    self.buildDataset(dataset_key, group_path, group_keydict,
                                      **kwargs)

        return group_name, group_path


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class ProvenanceBuilderMethods(FileBuilderMethods):

    def preInitBuilder(self, project_config, filetype, source, region, 
                             **kwargs):
        FileBuilderMethods.preInitBuilder(self, project_config, filetype,
                                                source, region, **kwargs)
        if "provenance" in self.config \
        and 'generators' in self.config.provenance:
            self.prov_generators = self.config.provenance.generators
        else: self.prov_generators = { }

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # build everything
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def build(self, build_groups=True, build_datasets=True, lons=None,
                    lats=None, **kwargs):

        # initiaize file attributes
        self.initFileAttributes(**kwargs)

        # build data groups
        groups = self.filetype.get('groups',None)
        if groups and build_groups:
            if self.debug: print 'building file level groups'
            for group_key in groups:
                if not self.groupExists(group_key):
                    self.open('a')
                    self.buildGroup(group_key, build_datasets, **kwargs)
                    self.close()

        # build file-level datasets
        datasets = self.filetype.get('datasets',None)
        # initialze lat and lon datasets if the data was passed
        if datasets and build_datasets:
            if lons is not None and lats is not None:
                self.initLonLatData(lons, lats)
                datasets = list(datasets)
                datasets.remove('lon')
                datasets.remove('lat')
        # test again, in case lats and lons were the only datasets in the list
        if datasets and build_datasets:
            if self.debug: print 'building file level datasets'
            for dataset_key in datasets:
                self.open('a')
                if 'provenance' in dataset_key:
                    self.buildProvenance(dataset_key, **kwargs)
                else: self.buildDataset(dataset_key, **kwargs)
                self.close()

        # return with file open for updates
        self.open('a')


    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #
    # data group initiailzation methods
    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def buildGroup(self, group_key, build_datasets, root_path=None, **kwargs):
        group_name, group, group_keydict = self._groupConfig(group_key)
        if self.groupExists(group_name, root_path): return

        if root_path is not None:
            group_path = '%s.%s' % (root_path, group_name)
        else: group_path = group_name
        if self.debug: print 'building %s group' % group_path

        attrs = \
            self._resolveGroupBuildAttributes(group, group_keydict, **kwargs)

        self.open('a')
        self.createGroup(group_path, **attrs)
        self.close()

        # build this group's sub-groups
        groups = group.get('groups', None)
        if groups:
            if self.debug: print 'building subgroups of', group_path
            for name in groups:
                self.buildGroup(name, build_datasets, group_path, **kwargs)

        # build this group's datasets
        datasets = group.get('datasets', None)
        if datasets and build_datasets:
            if self.debug: print 'building datasets in group', group_path
            for dataset_key in datasets:
                if 'provenance' in dataset_key:
                    self.buildProvenance(dataset_key, group_path=group_path,
                                         **kwargs)
                else:
                    self.buildDataset(dataset_key, group_path, group_keydict,
                                      **kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # dataset build methods
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def buildProvenance(self, provenance_key, **kwargs):
        dataset_name, reference, prov_config, prov_views = \
                                  self._provenanceConfig(provenance_key)
        group_name = kwargs.get('group_name', None)
        group_path = kwargs.get('group_path', group_name)
        if self.datasetExistsIn(dataset_name, group_path): return

        if group_path is not None:
            dataset_path = '%s.%s' % (group_path ,dataset_name)
        else: dataset_path = dataset_name

        attrs = self._resolveProvenanceBuildAttributes(prov_config, **kwargs)
        description = attrs.get('description', None)
        if description is None:
            if group_name is not None:
                attrs['description'] = 'Provenance for %s' % group_name
            else:
                attrs['description'] = 'Provenance for %s' % reference
        if reference is not None: attrs['reference'] = reference

        records = self.generateEmptyProvenance(prov_config, attrs)

        names = prov_config.names
        formats = prov_config.formats
        provenance = N.rec.fromrecords(records, shape=(len(records),),
                                       formats=formats, names=names)
        self.open('a')
        self.createDataset(dataset_path, provenance, raw=True)
        self.setDatasetAttributes(dataset_path, **attrs)
        self.close()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def generateEmptyProvenance(self, prov_config, attrs):
        num_records = prov_config.get('num_records',
                                      attrs.get('num_records', None))
        errmsg = 'Cannot determine number of provenance records from arguments'
        assert(num_records is not None), errmsg
        errmsg = 'Need number of records > 0 to generate empty provenance'
        assert(num_records > 0), errmsg
        empty_record = prov_config.empty
        return [empty_record for record in range(num_records)]

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #
    # provenance dataset config and generators
    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _provenanceConfig(self, provenance_key):
        reference = None
        if ':' in provenance_key:
            parts = provenance_key.split(':')
            if len(parts) == 2: name, key = parts
            elif len(parts) == 3: name, reference, key = parts
        else: name = key = provenance_key
        prov_config = self.config.provenance.types[key].copy()
        views = self.config.provenance.views
        return prov_config.get('path', name), reference, prov_config, views
    _getProvenanceConfig = _provenanceConfig # backwards compatibility

    def _provenanceGenerator(self, provenance_key):
        return self.prov_generators[provenance_key]
    _getProvenanceGenerator = _provenanceGenerator # backwards compatibility

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _resolveProvenanceBuildAttributes(self, prov_config, **kwargs):
        attrs = self._resolveCommonAttributes(**kwargs)
        attrs['description'] = prov_config.get('description', None)
        attrs['key'] = prov_config.name
        view = prov_config.names[0]
        attrs.update(self._resolveScopeAttributes(prov_config, **kwargs))
        attrs['generator'] = \
            kwargs.get('generator', prov_config.get('generator', attrs['key']))
        return attrs


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class TimeGridFileBuildMethods(ProvenanceBuilderMethods):
    """
    Provides additional methods required to build files containing time
    series grids.

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    #
    # REQUIRED - builder-specific initialization methods must be included
    #            in the __init__ method of any builder class
    #
    # preInitBuilder MUST be called by __init__ method of any builder
    # subclass prior to the superclass.__init__ method
    #
    # initFileAttributes MUST be called by __init__ method of any builder
    # subclass after the superclass.__init__ method
    #
    # postInitBuilder MUST be called by __init__ method after
    # initFileAttributes
    #
    # def __init__(self, project_config, filetype, source, ... , **kwargs):
    #
    #     self.preInitBuilder(project_config, filetype, source, **kwargs)
    #
    #     superclass.__init__(...)
    #
    #     self.initFileAttributes(**kwargs(
    #
    #     self.postInitBuilder(**kwargs)
    #
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    """

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # add DateGrid-specific initialization to base preInitBuilder
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def preInitBuilder(self, project_config, filetype, source, target_year,
                             region, **kwargs):
        ProvenanceBuilderMethods.preInitBuilder(self, project_config, filetype,
                                                      source, region, **kwargs)
        if target_year is not None:
            self.preInitTimeAttributes(target_year, **kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def preInitTimeAttributes(self, target_year, **kwargs):
        self.target_year = target_year
        if target_year in (365,366):
            self.end_day = self._projectEndDay(**kwargs)
            self.end_doy = self._projectEndDoy(target_year, **kwargs)
            self.start_day = self._projectStartDay(**kwargs)
            self.start_doy = self._projectStartDoy(target_year, **kwargs)
            self.num_days = (self.end_doy - self.start_doy) + 1
        else:
            self.end_date = self._projectEndDate(target_year,**kwargs)
            self.start_date = self._projectStartDate(target_year, **kwargs)
            self.num_days = (self.end_date - self.start_date).days + 1

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # additional file attributes (called by base initFileAttributes method)
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def additionalFileAttributes(self, **kwargs):
        attrs = { }
        attrs['target_year'] = self.target_year
        if self.target_year in (365,366):
            attrs['end_day'] = self.end_day
            attrs['end_doy'] = self.end_doy
            attrs['start_day'] = self.start_day
            attrs['start_doy'] = self.start_doy
        else:
            attrs['end_date'] = asAcisQueryDate(self.end_date)
            attrs['start_date'] = asAcisQueryDate(self.start_date)
        attrs['num_days'] = self.num_days
        return attrs

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    
    def generateEmptyProvenance(self, prov_config, attrs):
        time_view = prov_config.names[0]
        if time_view in prov_views.date:
            return self._generateEmptyDateProvenance(prov_config, attrs)
        elif time_view in prov_views.doy:
            return self._generateEmptyDoyProvenance(prov_config, attrs)
        else:
            empty_record = prov_config.empty
            return [empty_record for day in range(self.num_days)]

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _generateEmptyDateProvenance(self, provenance, attrs):
        records = [ ]
        record_tail = provenance.empty[1:]

        date = asDatetimeDate(attrs.get('start_date',self.start_date))
        while date <= self.end_date:
            record = (asAcisQueryDate(date),) + record_tail
            records.append(record)
            date += ONE_DAY
        return records

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _generateEmptyDoyProvenance(self, provenance, attrs):
        records = [ ]
        record_tail = provenance.empty[1:]
        doy_type = N.dtype(provenance.empty[0])
        start_doy = self._startDoy(attrs)
        for day_num in range(self.num_days):
            record = (doy_type.type(start_doy+day_num),) + record_tail
            records.append(record)
        return records

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #
    # attribute resolution methods
    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _shapeForDataset(self, dataset):
        source_dimensions = self._sourceDimensions()
        view = self._mappedDatasetView(dataset)

        shape = [ ]
        for dim in dataset.view:
            if isinstance(dim, basestring):
                if dim == 'time':
                    period = dataset.get('period','date')
                    if period in ('date','doy'):
                        shape.append(self.num_days)
                    elif period == 'year':
                        shape.append(self.num_years)
                elif dim in source_dimensions.attrs:
                    shape.append(source_dimensions[dim])
                else:
                    raise ValueError, "Cannot unravel dimension '%s'" % dim
            elif isinstance(dim, int):
                shape.append(dim)
            else:
                raise ValueError, "Cannot unravel dimension '%s'" % str(dim)
        return view, tuple(shape)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _resolveDateAttributes(self, dataset, **kwargs):
        time_attrs = { }
        for key, value in kwargs.items():
            if key.endswith('_date'):
                time_attrs[key] = asAcisQueryDate(value)
        if self.target_year:
            if 'start_date' not in time_attrs:
                date =self._projectStartDate(self.target_year, **dataset)
                time_attrs['start_date'] = asAcisQueryDate(date)
            if 'end_date' not in time_attrs:
                date = self._projectEndDate(self.target_year, **dataset)
                time_attrs['end_date'] = asAcisQueryDate(date)
        return time_attrs

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _resolveDoyAttributes(self, dataset, **kwargs):
        attrs = { }
        if self.target_year:
            if self.target_year == 365:
                year = 1971
            elif self.target_year == 366:
                year = 1972
            else: 
                year = self.target_year

            start_day = kwargs.get('start_day',
                        dataset.get('start_day', self.filetype.start_day))
            attrs['start_day'] = start_day
            end_day = kwargs.get('end_day',
                      dataset.get('end_day', self.filetype.end_day))
            attrs['end_day'] = end_day

            start_doy = datetime.date(year, *start_day).timetuple().tm_yday
            end_doy = datetime.date(year, *end_day).timetuple().tm_yday
            attrs['doy'] = (start_doy,end_doy)
        return attrs

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _resolveProvenanceBuildAttributes(self, prov_config, **kwargs):
        inherit = ProvenanceBuilderMethods
        attrs = \
        inherit._resolveProvenanceBuildAttributes(self, prov_config, **kwargs)
        view = attrs['view']
        if view in ('date','obs_date','obsdate'):
            attrs.update(self._resolveDateAttributes(prov_config, **kwargs))
        elif view == 'doy':
            attrs.update(self._resolveDoyAttributes(prov_config, **kwargs))
        else:
            errmsg = 'Cannot resolve proveance attributes for "%s" time units'
            raise ValueError, errmsg % view
        return attrs

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def _resolveScopeAttributes(self, dataset_config, **kwargs):
        inherit = ProvenanceBuilderMethods
        attrs = inherit._resolveScopeAttributes(self, dataset_config, **kwargs)
        #if period == 'date':
        #    attrs.update(self._resolveDateAttributes(dataset_config, **kwargs))
        #elif period == 'doy':
        #    attrs.update(self._resolveDoyAttributes(dataset_config, **kwargs))
        #else:
        if 'period' in attrs:
            if attrs['period'] not in ('date','doy'):
                attrs['interval'] = \
                     kwargs.get('interval', dataset_config.get('interval',1))
        return attrs

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _resolveTimeAttributes(self, dataset_config, **kwargs):
        period = dataset_config.get('period',None)
        if period is None:
            return  { }
        elif period == 'date':
            return self._resolveDateAttributes(dataset_config, **kwargs)
        elif period == 'doy':
            return self._resolveDoyAttributes(dataset_config, **kwargs)
        else:
            msg = 'dataset configuration contains an unsupported time period :'
            errmsg = '"%s" %s "%s"' % (dataset_config.name, errmsg, period)
            raise ValueError, errmsg
 
    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #
    # file time initialization methods
    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _projectEndDate(self, year, **kwargs):
        end_date = kwargs.get('end_date', None)
        if end_date is None:
            day = self._projectEndDay(**kwargs)
            end_date = datetime.date(year, *day)
        return end_date

    def _projectEndDay(self, **kwargs):
        day = kwargs.get('end_day', self.filetype.get('end_day',
                         self.config.project.get('end_day', None)))
        if day is not None: return day
        else:
            raise 'end day has not been configured for this project'

    def _projectEndDoy(self, year, **kwargs):
        if year == 366:
            date = self._projectEndDate(1972, **kwargs)
        elif year == 365:
            date = self._projectEndDate(1971, **kwargs)
        else: date = self._projectEndDate(year, **kwargs)
        return date.timetuple().tm_yday

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _projectStartDate(self, year, **kwargs):
        start_date = kwargs.get('start_date', None)
        if start_date is None:
            day = self._projectStartDay(**kwargs)
            return datetime.date(year, *day)
        else: return start_date

    def _projectStartDay(self, **kwargs):
        day = kwargs.get('start_day', self.filetype.get('start_day',
                         self.config.project.get('start_day', None)))
        if day is not None: return day
        else:
            raise 'start day has not been configured for this project'

    def _projectStartDoy(self, year, **kwargs):
        if year == 366:
            date = self._projectStartDate(1972, **kwargs)
        elif year == 365:
            date = self._projectStartDate(1971, **kwargs)
        else: date = self._projectStartDate(year, **kwargs)
        return date.timetuple().tm_yday
