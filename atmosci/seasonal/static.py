
from atmosci.hdf5.manager import Hdf5GridFileManager
from atmosci.hdf5.manager import Hdf5GridFileReader

from atmosci.seasonal.methods.builder import GridFileBuildMethods
from atmosci.seasonal.methods.grid import GridFileManagerMethods
from atmosci.seasonal.methods.grid import GridFileReaderMethods

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class StaticGridFileMethods:
    
    def gribSourceIndexes(self, grib_source='ndfd'):
        source_shape = self.datasetShape('%s.x_indexes' % grib_source)
        source_indexes = [self.getData('%s.y_indexes' % grib_source).flatten(),
                          self.getData('%s.x_indexes' % grib_source).flatten()]
        return source_shape, source_indexes


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class StaticGridFileBuilder(StaticGridFileMethods, GridFileBuildMethods,
                            Hdf5GridFileManager):

    def __init__(self, filepath, registry, project_config, filetype, source,
                       region, **kwargs):
        print '\nStaticGridFileBuilder\nregion config :', region, '\n', project_config.regions[region]

        self.preInitBuilder(project_config, filetype, source, region, **kwargs)
        self.registry = registry.copy()
        Hdf5GridFileManager.__init__(self, filepath, 'w')
        self.initFileAttributes(**kwargs)
        self.postInitBuilder(**kwargs)

        # leave file open in append mode
        self.open(mode='a')


    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _loadManagerAttributes_(self):
        Hdf5GridFileManager._loadManagerAttributes_(self)
        self._loadProjectFileAttributes_()


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class StaticGridFileManager(StaticGridFileMethods, GridFileManagerMethods,
                            GridFileReaderMethods, Hdf5GridFileManager):

    def __init__(self, filepath, registry, mode='r'):
        Hdf5GridFileManager.__init__(self, filepath, mode)


    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _loadManagerAttributes_(self):
        Hdf5GridFileManager._loadManagerAttributes_(self)
        self._loadProjectFileAttributes_()


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class StaticGridFileReader(StaticGridFileMethods, GridFileReaderMethods,
                           Hdf5GridFileReader):

    def __init__(self, filepath, registry):
        Hdf5GridFileReader.__init__(self, filepath)


    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _loadManagerAttributes_(self):
        Hdf5GridFileReader._loadManagerAttributes_(self)
        self._loadProjectFileAttributes_()

