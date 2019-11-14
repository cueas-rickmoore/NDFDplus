
import os

from atmosci.acis.griddata import AcisGridDownloadMixin

from atmosci.seasonal.factory import SeasonalSourceFileFactory,\
                                     NDFDProjectFactory

from atmosci.tempexts.grid import TemperatureFileReader, \
                                  TemperatureFileManager, \
                                  TemperatureFileBuilder

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from atmosci.tempexts.config import CONFIG
from atmosci.tempexts.registry import REGISTRY

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class TempextsFactoryMethods:

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def tempextsFilename(self, target_year, source, region, **kwargs):
        template = self.filenameTemplate('tempexts', None)
        if template is None:
            template = self.filenameTemplate(source)
            if template is None: template = self.filenameTemplate('source')
        template_args = { 'data_type':'tempexts',
                          'source':self.sourceToFilepath(source),
                          'year':target_year,
                        }
        if region is not None:
            template_args['region'] = self.regionToFilepath(region, False)
        if kwargs: template_args.update(dict(kwargs))
        return template % template_args
 
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def tempextsFilepath(self, target_year, source, region, **kwargs):
        filepath = kwargs.get('filepath', None)
        if filepath is not None: return filepath

        root_dir = self.tempextsDirpath(source, region, **kwargs)
        filename = self.tempextsFilename(target_year, source, region, **kwargs)
        return os.path.join(root_dir, filename)
 
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def tempextsDirpath(self, source, region, **kwargs):
        shared = self.project.get('shared_source', False)
        if shared:
            root_dir = self.sharedRootDir('grid')
        else:
            root_dir = self.config.dirpaths.get('tempexts',
                            self.config.dirpaths.get('source',
                            self.projectRootDir()))
        if self.project.subproject_by_region:
            root_dir =  self.subdirByRegion(root_dir, region)
        root_dir = \
            os.path.join(root_dir, self.sourceToDirpath(source))
        subdir = kwargs.get('subdir', None)
        if subdir is not None:
            root_dir = os.path.join(root_dir, subdir)
        else: root_dir = os.path.join(root_dir, 'temps')
        if not os.path.exists(root_dir): os.makedirs(root_dir)
        return root_dir

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def tempextsFileBuilder(self, target_year, source, region, start_date,
                                  end_date, **kwargs):
        Class = self.fileAccessorClass('tempexts', 'build')
        filepath = self.tempextsFilepath(target_year, source, region, **kwargs)
        return Class(filepath, self.registryConfig(), self.config, target_year,
                     source, region, start_date, end_date, **kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def tempextsFileManager(self, target_year, source, region, mode='r',
                                  **kwargs):
        Class = self.fileAccessorClass('tempexts', 'manage')
        filepath = self.tempextsFilepath(target_year, source, region, **kwargs)
        return Class(filepath, self.registryConfig(), mode)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def tempextsFileReader(self, target_year, source, region, **kwargs):
        Class = self.fileAccessorClass('tempexts', 'read')
        filepath = self.tempextsFilepath(target_year, source, region, **kwargs)
        return Class(filepath, self.registryConfig())

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def useDevEnv(self):
        dev_dirpaths = self.config.get('dev_dirpaths', None)
        if dev_dirpaths is not None:
            self.config.dirpaths.update(dev_dirpaths.attrs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _registerTempsFileAccessClasses(self):
        self._registerAccessManagers('tempexts', TemperatureFileReader,
                                     TemperatureFileManager,
                                     TemperatureFileBuilder)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class TempextsProjectFactory(TempextsFactoryMethods, AcisGridDownloadMixin,
                             SeasonalSourceFileFactory):

    def __init__(self, config=CONFIG, registry=REGISTRY):
        SeasonalSourceFileFactory.__init__(self, config, registry)
    
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _registerAccessClasses(self):
        self._registerTempsFileAccessClasses()
        # some older code uses 'source' for temperature files
        self.AccessClasses.tempexts.copy('source', self.AccessClasses)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class TempextsForecastFactory(TempextsFactoryMethods, NDFDProjectFactory):

    def __init__(self, config=CONFIG, registry=REGISTRY):
        NDFDProjectFactory.__init__(self, config, registry)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _registerAccessClasses(self):
        self._registerTempsFileAccessClasses()
        # some older code uses 'source' for temperature files
        self.AccessClasses.tempexts.copy('source', self.AccessClasses)

