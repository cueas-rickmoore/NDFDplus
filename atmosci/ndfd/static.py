# Copyright (c) 2007-2018 Rick Moore and Cornell University Atmospheric
#                         Sciences
# All Rights Reserved
# Principal Author : Rick Moore
#
# ndfd is part of atmosci - Scientific Software for Atmosphic Science
#
# see copyright.txt file in this directory for details

import numpy as N

from atmosci.hdf5.grid import Hdf5GridFileManager
from atmosci.hdf5.grid import Hdf5GridFileBuilderMixin

from atmosci.seasonal.registry import REGBASE
#from atmosci.seasonal.static import StaticGridFileBuilder
#from atmosci.seasonal.static import StaticGridFileMethods
#from atmosci.seasonal.methods.grid import GridFileManagerMethods
from atmosci.seasonal.methods.builder import GridFileBuildMethods

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

#class NdfdStaticGridFileBuilder(StaticGridFileBuilder):
class NdfdStaticGridFileBuilder(GridFileBuildMethods,
                                Hdf5GridFileManager):
    """ Creates a new HDF5 file with read/write access to hourly 3D
        gridded data.
    """
    def __init__(self, conus_filepath, ndfd_filepath, config, region, **kwargs):
        filetype = kwargs.get('filetype', 'ndfd_static')
        source = kwargs.get('source', config.sources.ndfd.grid)
        registry = kwargs.get('registry', REGBASE)

        self.preInitBuilder(config, 'ndfd_static', source, region, **kwargs)
        self.registry = registry.copy()
        Hdf5GridFileManager.__init__(self, ndfd_filepath, 'w')
        self.initFileAttributes(**kwargs)
        self.postInitBuilder(**kwargs)

        # initiaize file attributes
        self.initFileAttributes(**kwargs)

        # initiaize builder specific attributes
        self.initBuilderAttributes(conus_filepath, **kwargs)

        # create a reader for the CONUS static file
        self.conus_reader = None
        self.conus_filepath = conus_filepath

        # make sure the user has write access
        mode = kwargs.get('mode', 'w')
        if mode == 'w':
            self.load_manager_attrs = False
        else: self.load_manager_attrs = True
        self.time_attr_cache = { }
        # close the file to make sure attributes are saved
        self.close()

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def buildAcisDatasets(self, **kwargs):
        debug = kwargs.get('debug', self.debug)

        print '\nbuilding ACIS file level datasets'
        datasets = list(self.config.static.ndfd.datasets)

        # get indexes for bounding box
        bbox = self.datasetBounds()
        [min_y, max_y, min_x, max_x] = bbox
        if debug:
            print '    initializing Lat/Lon datasets'
            print '        bounds :', min_y, max_y, min_x, max_x

        reader = self.conusReader()
        lats = reader._slice2DDataset(reader.lats, min_y, max_y, min_x, max_x)
        shape = lats.shape
        
        kwargs['shape'] = shape
        self.open(mode='a')
        self.initStaticDataset('lat', lats, True, None, **kwargs)
        self.close()

        lons = reader._slice2DDataset(reader.lons, min_y, max_y, min_x, max_x)
        self.open(mode='a')
        self.initStaticDataset('lon', lons, True, None, **kwargs)
        self.close()

        datasets.remove('lon')
        datasets.remove('lat')

        self.open(mode='a')
        for dataset in datasets:
            data = reader.getData(dataset)
            data = reader._slice2DDataset(data, min_y, max_y, min_x, max_x)
            kwargs['shape'] = shape
            self.initStaticDataset(dataset, data, True, None, **kwargs)
        self.close()

        self.open(mode='a')
        for mask in self.config.static.ndfd.get('masks'):
            mask_type, dataset = mask.split(':')
            data = reader.getData(dataset)
            data = reader._slice2DDataset(data, min_y, max_y, min_x, max_x)
            kwargs['shape'] = shape
            self.initStaticDataset(mask, data, False, None, **kwargs)
        self.close()

        reader.close()

        return bbox, shape

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def buildNdfdGroup(self, **kwargs):
        debug = kwargs.get('debug', self.debug)

        print '\nbuilding NDFD group and datasets'

        # create the group, but empty
        if self.debug: print '    creating NDFD group'
        self.open(mode='a')
        group_name, path = self.buildGroup('ndfd_static', False, None, **kwargs)
        self.close()

        datasets = list(self.config.groups.ndfd_static.datasets)

        # get indexes for bounding box
        bbox = kwargs.get('bbox', None)
        shape = kwargs.get('shape', None)
        if bbox is None:
           bbox, shape = self.datasetBounds()
        if debug:
            print '\nndfd group bbox :', bbox
            print '            shape :', shape
  
        [min_y, max_y, min_x, max_x] = bbox
        if shape is None:
            shape = ( (max_y-min_y)+1, (max_x-minx)+1 )
            print ' shape after bbox applied :', shape

        if self.debug:
            print '    initializing %s Lat/Lon datasets' % group_name
        # subset lat/lon grids
        reader = self.conusReader()

        ndfd_lat = reader.getData('ndfd.lat')
        lats = self._slice2DDataset(ndfd_lat, min_y, max_y, min_x, max_x)
        kwargs['shape'] = shape
        self.open(mode='a')
        self.initStaticDataset('lat', lats, True, group_name, **kwargs)
        self.close()
        del ndfd_lat, lats

        self.open(mode='a')
        ndfd_lon = reader.getData('ndfd.lon')
        lons = self._slice2DDataset(ndfd_lon, min_y, max_y, min_x, max_x)
        kwargs['shape'] = shape
        self.initStaticDataset('lon', lons, True, group_name, **kwargs)
        self.close()
        del ndfd_lon, lons

        datasets.remove('lon')
        datasets.remove('lat')

        if self.debug:
            print '    initializing %s datasets' % group_name
            print '        ', datasets

        template = '%s.%%s' % group_name
        for dataset_key in datasets:
            dataset = self.config.datasets[dataset_key]
            dsname = dataset.get('path', dataset_key)
            data = reader.getData(template % dsname)
            data = self._slice2DDataset(data, min_y, max_y, min_x, max_x)
            kwargs['shape'] = shape
            self.open(mode='a')
            self.initStaticDataset(dataset_key, data, True, group_name, **kwargs)
            self.close()

        reader.close()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def conusReader(self):
        if self.conus_reader is None:
            conus_reader = Hdf5GridFileManager(self.conus_filepath, 'r')
            conus_reader.lats = conus_reader.getData('lat')
            conus_reader.lons = conus_reader.getData('lon')
            self.conus_reader = conus_reader
        elif not self.conus_reader.isOpen(): 
            self.conus_reader.open('r')
            conus_reader = self.conus_reader
            conus_reader.lats = conus_reader.getData('lat')
            conus_reader.lons = conus_reader.getData('lon')
        return self.conus_reader

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def datasetBounds(self):
        bbox = self.bbox
        reader = self.conusReader()
        min_y, min_x = reader._indexOfClosestNode(bbox[0], bbox[1], self.tolerance)
        max_y, max_x = reader._indexOfClosestNode(bbox[2], bbox[3],  self.tolerance)
        return [min_y, max_y, min_x, max_x]

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def initBuilderAttributes(self, conus_filepath, **kwargs):
        # common bounding box for all data
        bbox = self.region.data
        if isinstance(bbox, str):
            self.bbox = [float(coord) for coord in bbox.split(',')] 
        else:  #self.bbox = [bbox[0], bbox[2], bbox[1], bbox[3]]
            self.bbox = list(bbox)

        self.dataset_indexes = None

        # search distance tolerance
        self.tolerance = kwargs.get('tolerance', self.config.project.bbox_tolerance)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def sourceFileAttributes(self, **kwargs):
        """
        resolve data source attributes for the file
        """ 
        source = self.source
        region = kwargs.get('region', source.region)
        attrs = { 'bbox':kwargs.get('bbox', source.bbox[region]),
                  'description':kwargs.get('description', source.description),
                  'grid_type': source.grid_type,
                  'node_spacing': source.node_spacing,
                  'region': region,
                  'search_radius': source.search_radius,
                  'source': self.filetype.get('source', source.tag),
                }

        node_spacing = source.get('node_spacing',None)
        if node_spacing is not None:
            attrs['node_spacing'] = node_spacing 
        search_radius = source.get('search_radius',None)
        if search_radius is not None:
            attrs['search_radius'] = search_radius

        return attrs

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    
    def _datasetConfig(self, dataset_key, **kwargs):
        if ':' in dataset_key:
            if 'mask' in dataset_key:
                key, name = dataset_key.split(':')
            else: name, key = dataset_key.split(':')
        else: key = name = dataset_key
        
        dataset = self.config.datasets[key]
        name = dataset.get('path', name)
        base = dataset.get('base',None)
        if base is not None:
            dataset.inheritAttrs(self.config.datasets[base])

        return name, dataset, None
    
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _fileDescription(self, source, **kwargs):
        return kwargs.get('source_description',
                   self.filetype.get('description', source.description))

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _resolveSourceAttributes(self, **kwargs): 
        """
        resolve data source attributes for a dataset
        """
        source = self.source
        attrs = {
                  'grid_type': kwargs.get('grid_type', source.grid_type),
                  'node_spacing': source.node_spacing,
                  'region': kwargs.get('region', source.region),
                  'resolution': kwargs.get('resolution', source.resolution),
                  'node_search_radius': source.search_radius,
                  'source': source.tag,
                }
        return attrs

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _loadManagerAttributes_(self):
        Hdf5GridFileManager._loadManagerAttributes_(self)
        self._loadProjectFileAttributes_()

