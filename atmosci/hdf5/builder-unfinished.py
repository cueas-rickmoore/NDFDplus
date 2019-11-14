
import datetime
from copy import copy, deepcopy

import numpy as N

from atmosci.utils.config import ConfigObject
from atmosci.utils.timeutils import asDatetimeDate, asAcisQueryDate, ONE_DAY

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class GridFileBuilder(object):
    """ Provides common methods requred to build files containing grids.
    """

    def preInitBuilder(self, config, filetype, source, region, **kwargs):
        """
        Initializes common build properties for time-based grids
        
        Arguments:
        ---------
        config: an instance of atmosci.utils.config.ConfigObject with
                definitions of the project's file structure and data
                sources. See atmosci.seasonal.config for an example.
        filetype: file config object or the name of a file type defined
                  in the "filetypes" element of the config object.
        source: data source config object or the name of a data source
                defined in the "sources" element of the config object.
        region: data region config object or the name of a data region
                defined in the "regions" element of the config object.

        Returns: None
        """
        self.__config = config.copy('config', None)
        self.__project = \
             kwargs.get('project',self.__config.get("project",None))
        
        if isinstance(filetype, ConfigObject):
            self.__filetype = filetype
        elif isinstance(filetype, basestring):
            self.__filetype = self.__config.filetypes[filetype]

        if isinstance(source, ConfigObject):
            self.__source = source
        elif isinstance(source, basestring):
            self.__source = self.__config.sources[source]

        if isinstance(region, ConfigObject):
            self.__region = region
        elif isinstance(region, basestring):
            self.__region = self.__config.regions[region]


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
        return self.__project

    @property
    def region(self):
        return self.__region

    @property
    def source(self):
        return self.__source


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
                self.buildDataset(dataset_key, **kwargs)
                self.close()

        # return with file open for updates
        self.open('a')


    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #
    # data group initiailzation methods
    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def buildGroup(self, group_key, build_datasets, root_path=None, **kwargs):
        group_name, group, group_keydict = self._groupConfig(group_key)
        if group_name not in self._group_names:
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
                    self.buildGroup(name, group_path, **kwargs)

            # build this group's datasets
            datasets = group.get('datasets', None)
            if datasets and build_datasets:
                if self.debug: print 'building datasets in group', group_path
                for dataset_key in datasets:
                    if 'provenance' in dataset_key:
                        self.buildProvenance(dataset_key, group_path=group_path,
                                             **kwargs)
                    else:
                        self.buildDataset(dataset_key, group_path,
                                          group_keydict, **kwargs)


    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #
    # dataset build methods
    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def buildDataset(self, dataset_key, group_path=None, group_keydict=None,
                           **kwargs):
        dataset_name, dataset_config, dataset_keydict = \
                              self._datasetConfig(dataset_key, **kwargs)

        if group_path is not None:
            dataset_path = '%s.%s' % (group_path, dataset_name)
        else: dataset_path = dataset_name

        shape, dtype, attrs = \
        self._resolveDatasetBuildAttributes(dataset_config, dataset_keydict,
                                            group_keydict, **kwargs)
        if 'shape' in kwargs: del kwargs['shape']

        view = attrs.get('view',None)
        chunks = \
            self._resolveDatasetChunks(dataset_config, shape, view, **kwargs)
        if chunks is not None:
            attrs['chunks'] = chunks
            attrs['compression'] = dataset_config.get('compression', 'gzip')

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

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def initLonLatData(self, lons, lats, **kwargs):
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
        return name, dataset, keys

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _fileDescription(self, source_tag, **kwargs):
        template = self._fileDescriptionTemplate(self.filetype)
        descrip_dict = copy(kwargs)
        if 'source' not in descrip_dict: 
            descrip_dict['source'] = source_tag
        return template % descrip_dict

    def _fileDescriptionTemplate(self, filetype):
        if isinstance(filetype, basestring):
            filetype_cfg = self.config.filetypes[filetype]
        else: filetype_cfg = filetype
        template = filetype_cfg.get('description',
                   '%%(source)s %s' % filetype.name.title())
        return template

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

    def _mappedView(self, dataset):
        return self.config.view_map.attrs[dataset.view]

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

        source_tag = kwargs.get('source_tag', self.source.tag)
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
        attrs['description'] = dataset_config.description % _keydict_
        attrs.update(self._resolveSourceAttributes(**kwargs))
        attrs.update(self._resolveScopeAttributes(dataset_config, **kwargs))
 
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
        if period == 'date':
            attrs.update(self._resolveDateAttributes(dataset_config, **kwargs))
        elif period == 'doy':
            attrs.update(self._resolveDoyAttributes(dataset_config, **kwargs))
        else:
            attrs['interval'] = kwargs.get('interval', dataset_config.get('interval',1))
        return attrs
 
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _resolveSourceAttributes(self, **kwargs):
        source = self.source
        attrs = { 'source':kwargs.get('source_tag', source.tag) }

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

    def _shapeForDataset(self, dataset):
        errmsg = "Cannot unravel dimension '%s'"
        source_dimensions = self._sourceDimensions()
        view = self._mappedView(dataset)

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

class TimeGridFileBuilder(GridFileBuilder):
    """ Provides additional methods required to build files containing time
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

    def preInitBuilder(self, config, filetype, target_year, source, reqion,
                             **kwargs):
        """
        Initializes common build properties for time-based grids
        
        Arguments:
        ---------
        config: an instance of atmosci.utils.config.ConfigObject with
                definitions of the project's file structure and data
                sources. See atmosci.seasonal.config for an example.
        filetype: a string describing the type of file to be created.
                  This should be a key to a filetype described in the
                  "filetypes" element of the config file.
        target_year: year in which this project begins
        source: data source config object or the name of a data source
                defined in the input config object
        region: data region config object or the name of a data region
                defined in the input config object

        Returns: None
        """
        GridFileBuilder.preInitBuilder(self, project_config, filetype, source,
                                           region, **kwargs):
        if "provenance" in self.config:
            self.prov_generators = self.config.provenance.generators
        else: self.prov_generators = { }

        self.preInitTimeAttributes(target_year, **kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # provenance tracking methods
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def buildProvenance(self, provenance_key, **kwargs):
        dataset_name, prov_config, prov_views = \
                                  self._provenanceConfig(provenance_key)

        group_name = kwargs.get('group_name', None)
        group_path = kwargs.get('group_path', group_name)
        if group_path is not None:
            dataset_path = '%s.%s' % (group_path ,dataset_name)
        else: dataset_path = dataset_name

        attrs = self._resolveProvenanceBuildAttributes(prov_config, **kwargs)
        description = attrs.get('description', None)
        if description is None:
            if group_name is not None:
                attrs['description'] = 'Provenance for %s' % group_name
            else: attrs['description'] = 'Provenance for %s' % dataset_name

        names = prov_config.names
        time_view = names[0]
        if time_view in prov_views.date:
            records = self._generateEmptyDateProvenance(prov_config, attrs)
        elif time_view in prov_views.doy:
            records = self._generateEmptyDoyProvenance(prov_config, attrs)
        else:
            empty_record = prov_config.empty
            records = [empty_record for day in range(num_days)]

        formats = prov_config.formats
        provenance = N.rec.fromrecords(records, shape=(len(records),),
                                       formats=formats, names=names)
        self.open('a')
        self.createDataset(dataset_path, provenance, raw=True)
        self.setDatasetAttributes(dataset_path, **attrs)
        self.close()


    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #
    # provenance dataset config and generators
    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

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
        for day_num in range(num_days):
            record = (doy_type.type(start_doy+day_num),) + record_tail
            records.append(record)
        return records

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _provenanceConfig(self, provenance_key):
        if ':' in provenance_key:
            name, key = provenance_key.split(':')
        else: name = key = provenance_key
        prov_config = self.config.provenance.types[key].copy()
        views = self.config.provenance.views
        return prov_config.get('path', name), prov_config, views

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _provenanceGenerator(self, provenance_key):
        return self.prov_generators[key]

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #
    # attribute resolution methods
    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _resolveDatasetBuildAttributes(self, dataset_config, keydict=None,
                                             group_keydict=None, **kwargs):
        shape, packed_dtype, attrs =\
            GridFileBuilder._resolveDatasetBuildAttributes(self,
                            dataset_config, keydict, group_keydict, **kwargs)

        time_attrs = \
            self._resolveDatasetTimeAttributes(dataset_config, **kwargs)
        attrs.update(time_attrs)
        
        return shape, packed_dtype, attrs
 
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _resolveDatasetTimeAttributes(self, dataset_config, **kwargs):
        attrs = { }
        return attrs

    def _resolveFileTimeAttributes(self, dataset_config, **kwargs):
        attrs = { }
        return attrs

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _resolveProvenanceBuildAttributes(self, prov_config, **kwargs):
        attrs = self._resolveCommonAttributes(**kwargs)
        attrs['description'] = prov_config.get('description', None)
        attrs['key'] = prov_config.name
        view = prov_config.names[0]
        attrs.update(self._resolveScopeAttributes(prov_config, **kwargs))
        attrs['generator'] = \
            kwargs.get('generator', prov_config.get('generator', attrs['key']))
        if view in ('date','obs_date','obsdate'):
            attrs.update(self._resolveDateAttributes(prov_config, **kwargs))
        elif view == 'doy':
            attrs.update(self._resolveDoyAttributes(prov_config, **kwargs))
        else:
            errmsg = 'Cannot resolve proveance attributes for "%s" time units'
            raise ValueError, errmsg % view
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
 
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _shapeForDataset(self, dataset):
        source_dimensions = self._sourceDimensions()
        view = self._mappedView(dataset)

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
