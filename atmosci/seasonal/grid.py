
from atmosci.hdf5.dategrid import Hdf5DateGridFileReader,\
                                  Hdf5DateGridFileManager,\
                                  Hdf5DateGridFileBuilder

from atmosci.seasonal.methods.builder  import TimeGridFileBuildMethods
from atmosci.seasonal.methods.timegrid import TimeGridFileReaderMethods,\
                                              TimeGridFileManagerMethods

from atmosci.seasonal.methods.grid import hdf5ReaderPatch

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from atmosci.seasonal.registry import REGBASE

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class SeasonalGridFileReader(TimeGridFileReaderMethods,
                             Hdf5DateGridFileReader):

    def __init__(self, filepath, registry=None):
        if registry is None: self._preInitProject_(REGBASE)
        else: self._preInitProject_(registry)
        Hdf5DateGridFileReader.__init__(self, filepath)
        hdf5ReaderPatch(self)
        self._postInitProject_()

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _loadManagerAttributes_(self):
        Hdf5DateGridFileReader._loadManagerAttributes_(self)
        self._loadProjectFileAttributes_()


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class SeasonalGridFileManager(TimeGridFileManagerMethods,
                              Hdf5DateGridFileManager):

    def __init__(self, filepath, registry=None, mode='r'):
        if registry is None: self._preInitProject_(REGBASE)
        else: self._preInitProject_(registry)
        Hdf5DateGridFileManager.__init__(self, filepath, mode)
        hdf5ReaderPatch(self)
        self._postInitProject_()

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _loadManagerAttributes_(self):
        Hdf5DateGridFileManager._loadManagerAttributes_(self)
        self._loadProjectFileAttributes_()


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class SeasonalGridFileBuilder(TimeGridFileBuildMethods,
                              SeasonalGridFileManager):

    def __init__(self, filepath, registry, project_config, filetype, source,
                       target_year, region, **kwargs):

        self.preInitBuilder(project_config, filetype, source, target_year,
                            region, **kwargs)
        SeasonalGridFileManager.__init__(self, filepath, registry, 'w')
        self.initFileAttributes(**kwargs)
        self.postInitBuilder(**kwargs)
        self.close()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def updateDataset(self, dataset_path, start_time, data, **kwargs):
        self.open('a')
        SeasonalGridFileManager.updateDataset(self, dataset_path, start_time,
                                                    data, **kwargs)
        self.close()

    def updateProvenance(self, dataset_path, start_time, *data, **kwargs):
        self.open('a')
        SeasonalGridFileManager.updateProvenance(self, dataset_path, start_time,
                                                       *data, **kwargs)
        self.close()


    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _loadManagerAttributes_(self):
        SeasonalGridFileManager._loadManagerAttributes_(self)
        self._loadProjectFileAttributes_()


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class SeasonalDateGridFileBuilder(TimeGridFileBuildMethods,
                                  TimeGridFileManagerMethods,
                                  Hdf5DateGridFileBuilder):

    def __init__(self, filepath, filetype, registry, project_config,
                       target_year, start_date, end_date, source, region,
                       mode='w', **kwargs):
        self._preInitProject_(registry)
        self.preInitBuilder(project_config, filetype, source, target_year,
                            region, start_date=start_date, end_date=end_date,
                            **kwargs)
        lats = kwargs.get('lats',None)
        lons = kwargs.get('lons',None)
        Hdf5DateGridFileBuilder.__init__(self, filepath, start_date, end_date,
                                               lons, lats, mode)
        hdf5ReaderPatch(self)
        self.initFileAttributes(**kwargs)
        self.postInitBuilder(**kwargs)
        self.close()

