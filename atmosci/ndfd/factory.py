
import os
import datetime
import urllib

from atmosci.utils import tzutils
from atmosci.utils.config import ConfigObject
from atmosci.utils.timeutils import lastDayOfMonth

from atmosci.seasonal.methods.access  import BasicFileAccessorMethods
from atmosci.seasonal.methods.factory import MinimalFactoryMethods
from atmosci.seasonal.methods.paths   import PathConstructionMethods
from atmosci.seasonal.methods.source  import SourceFileAccessorMethods
from atmosci.seasonal.methods.static  import StaticFileAccessorMethods


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from atmosci.ndfd.config import CONFIG


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# just-in-time registration of static file access classe
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def _registerStaticFileReader(factory):
    from atmosci.seasonal.static import StaticGridFileReader
    factory._registerAccessManager('static', 'read', StaticGridFileReader)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class NDFDFactoryMethods(StaticFileAccessorMethods, PathConstructionMethods,
                         BasicFileAccessorMethods, MinimalFactoryMethods):
    """ Methods for managing grib files from NDFD and generating directory
    and file paths for the downloaded NDFD grib files.
    """

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def asFileTime(self, time_obj):
        return tzutils.asHourInTimezone(time_obj, self.file_tzinfo)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def asLocalTime(self, time_obj):
        return tzutils.asHourInTimezone(time_obj, self.local_tzinfo)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def fileAccessorClass(self, file_type, access_type):
        Classes = self.AccessClasses.get(file_type, None)
        if Classes is None or access_type not in Classes:
            self._registerNdfdFileAccessor(file_type, access_type)
            Classes = self.AccessClasses[file_type]
        return Classes[access_type]

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def monthTimespan(self, reference_date, **kwargs):
        if reference_date.day == 1: ref_date = reference_date
        else: ref_date = reference_date.replace(day=1) 
        ref_time = \
            datetime.datetime.combine(ref_date,datetime.time(hour=0))

        timezone = kwargs.get('timezone', self.file_tzinfo)
        start_time = ref_time = tzutils.asHourInTimezone(ref_time, timezone)
        num_days = lastDayOfMonth(ref_date.year, ref_date.month)
        end_time = start_time.replace(day=num_days, hour=23)

        return start_time, end_time

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def setFileTimezone(self, timezone):
        if tzutils.isValidTimezone(timezone):
            self.file_timezone = tzutils.timezoneAsString(timezone)
            self.file_tzinfo = timezone
        else:
            self.file_timezone = timezone
            self.file_tzinfo = tzutils.asTimezoneObj(timezone)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def setLocalTimezone(self, timezone):
        if tzutils.isValidTimezone(timezone):
            self.local_timezone = tzutils.timezoneAsString(timezone)
            self.local_tzinfo = timezone
        else:
            self.local_timezone = timezone
            self.local_tzinfo = tzutils.asTimezoneObj(timezone)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    
    def sourceConfigIt(self, source=None, for_ndfd=True):
        key = source_key
        if for_ndfd == True:
            if key is None: source = self.grib_source
            else: source = self.ndfd.get(key, None)
        else:
            if key is None: 
                source = self.config.project.get('source', None)
                if source is None: 
                    errmsg = "Source key was not passed to function and no default\n" 
                    errmsg = "%s was found in the project configuration." % errmsg
                    raise ValueError, errmsg
            else: source = self.config.sources.get(key, None)

        if source is not None: return source

        errmsg = "%s does not correspond to any configured source."
        raise KeyError, errmsg % key

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def timeOfLatestForecast(self):
        latest_time = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
        return latest_time.replace(minute=0, second=0, microsecond=0)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def variableConfig(self, variable, period='001-003'):
        return self.ndfd.variables[period][variable.lower()]

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _registerAccessClasses(self):
        if not hasattr(self, 'AccessClasses'):
            self.AccessClasses = ConfigObject('AccessClasses', None)
        if not hasattr(self, 'AccessRegistrars'):
            self.AccessRegistrars = ConfigObject('AccessRegistrars', None)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _registerNdfdFileAccessor(self, file_type, access_type):
        self.AccessRegistrars[file_type][access_type](self)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _initNdfdFactory_(self, config_object, **kwargs):
        self.ndfd = config_object.sources.ndfd

        self.grib_config = grib_config = self.ndfd.grib
        grib_src = kwargs.get('grib_source', grib_config.default_source)
        self.grib_source = self.ndfd[grib_src]
        timezone = kwargs.get('local_timezone', self.project.local_timezone)
        self.setLocalTimezone(timezone)

        if not hasattr(self, 'AccessRegistrars'):
            self.AccessRegistrars = ConfigObject('AccessRegistrars', None)
        # static file reader must be registered on init because code in
        # StaticFileAccessorMethods doesn't support just-in-time registration
        _registerStaticFileReader(self)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# just-in-time registration of grib file access classes
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def _registerNdfdGribIterator(factory):
    from atmosci.ndfd.grib import NdfdGribFileIterator
    factory._registerAccessManager('ndfd_grib', 'iter', NdfdGribFileIterator)

def _registerNdfdGribReader(factory):
    from atmosci.ndfd.grib import NdfdGribFileReader
    factory._registerAccessManager('ndfd_grib', 'read', NdfdGribFileReader)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class NdfdGribFactoryMethods(NDFDFactoryMethods):

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def datasetName(self, variable, period='001-003'):
        return self.ndfd.variables[period][variable.lower()].grib_dataset

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def ndfdDownloadDir(self, fcast_date):
        # determine root directory of forecast tree
        shared_grib_dir = self.project.get('shared_grib_dir',
                               self.project.get('shared_forecast',
                                    self.ndfd.get('shared_grib_dir', False)))
        if shared_grib_dir:
            fcast_dir = os.path.join(self.sharedRootDir(), 'forecast')
        else:
            fcast_dir = self.config.dirpaths.get('forecast', default=None)
            if fcast_dir is None:
                fcast_dir = os.path.join(self.projectRootDir(), 'forecast')

        download_dir_template = self.grib_config.file_subdirs
        # add subdirectory for forecast source
        download_dir = \
            os.path.join(fcast_dir, self.sourceToDirpath(self.ndfd))
        # add subdirectory for forecast date
        download_dir = \
            os.path.join(download_dir, self.timeToDirpath(fcast_date))
        # make sure directory exists before return
        if not os.path.exists(download_dir): os.makedirs(download_dir)
        return download_dir
    ndfdGribDirpath = ndfdDownloadDir

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def ndfdGribFileAccessor(self, access_type):
        Classes = self.AccessClasses.get('ndfd_grib', None)
        if Classes is None or access_type not in Classes:
            self._registerNdfdGridAccessor(access_type)
        return self.AccessClasses.ndfd_grid[access_type]

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def ndfdGribFilename(self, fcast_date, variable, period, **kwargs):
        grib_source = kwargs.get('source', self.ndfd.default_source)
        template_args = { 'date': self.timeToDirpath(fcast_date),
                          'month': fcast_date.strftime('%Y%m'),
                          'source': self.sourceToFilepath(grib_source),
                          'period': period,
                          'variable': variable.lower(),
                        }
        return self.ndfd.grib.file_template % template_args

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def ndfdGribFilepath(self, fcast_date, variable, period, **kwargs):
        forecast_dir = self.ndfdDownloadDir(fcast_date)
        filename = \
           self.ndfdGribFilename(fcast_date, variable, period, **kwargs)
        return os.path.join(forecast_dir, filename)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def ndfdGribIterator(self, fcast_date, variable, period, **kwargs):
        filepath = \
            self.ndfdGribFilepath(fcast_date, variable, period, **kwargs)
        Class = self.fileAccessorClass('ndfd_grib','iter')
        return Class(filepath)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def ndfdGribReader(self, fcast_date, variable, period, **kwargs):
        filepath = \
            self.ndfdGribFilepath(fcast_date, variable, period, **kwargs)
        Class = self.fileAccessorClass('ndfd_grib','read')
        return Class(filepath, period, **kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def setNdfdGribSource(self, source):
        self.ndfd_source = ndfd_source = self.ndfd[source]
        self.ndfd_periods = ndfd_source.periods

        self.ndfd_server = ndfd_source.server_url
        subdirs = ndfd_source.server_subdirs
        if isinstance(subdirs, basestring):
            self.ndfd_server_subdirs = subdirs
        else: self.ndfd_server_subdirs = os.path.join(subdirs)

        self.grib_config = ndfd_config = self.config.sources.ndfd.grib
        self.ndfd_file_template = ndfd_config.file_template
        self.ndfd_grib_subdirs = ndfd_config.subdirs

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _initNdfdGribFactory_(self, config_object, **kwargs):
        self._initNdfdFactory_(config_object, **kwargs)
        self.setNdfdGribSource(kwargs.get('source', self.ndfd.default_source))
        self.setFileTimezone(kwargs.get('grib_timezone',
                                        self.ndfd.grib.timezone))

        self.AccessRegistrars.ndfd_grib = { 'iter': _registerNdfdGribIterator,
                                            'read': _registerNdfdGribReader }


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class NdfdGribFileFactory(NdfdGribFactoryMethods, object):
    """
    Factory for downloading and accessing data in NDFD grib files.
    """
    def __init__(self, config_object=CONFIG, **kwargs):
        # initialize common configuration structure
        self._initFactoryConfig_(config_object, None, 'project')

        # initialize reanalysis grib-specific configuration
        self._initNdfdGribFactory_(config_object, **kwargs)

        # simple hook for subclasses to initialize additonal attributes  
        self.completeInitialization(**kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def downloadLatestForecast(self, variables, periods=('001-003','004-007'),
                                     region='conus', verbose=False):
        target_date = self.timeOfLatestForecast()
        url_template = self.ndfdUrlTemplate()
        template_args = {'region':region.lower(), }

        filepaths = [ ]
        for variable in variables:
            template_args['variable'] = variable
            for period in periods:
                template_args['period'] = period
                ndfd_url = url_template % template_args
                if verbose: print '\ndownloading :', ndfd_url
                local_filepath = self.ndfdGribFilepath(
                                      target_date, period, filetype)
                if verbose: print 'to :', local_filepath
            
                urllib.urlretrieve(ndfd_url, local_filepath)
                filepaths.append(local_filepath)

        return target_date, tuple(filepaths)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def ndfdUrlTemplate(self):
        return '/'.join( (self.ndfd_server, self.ndfd_subdir_path,
                          self.ndfd_file_template) )

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def setDownloadAttempts(self, attempts):
        if isinstance(attempts, int): self.wait_attempts = attempts
        else: self.wait_attempts = int(attempts)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def setDownloadWaitTime(self, seconds):
        if isinstance(seconds, float): self.wait_seconds = seconds
        else: self.wait_seconds = float(seconds)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# just-in-time registration of grid file access classes
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def _registerNdfdGridBuilder(factory):
    from atmosci.ndfd.grid import NdfdGridFileBuilder
    factory._registerAccessManager('ndfd_grid', 'build', NdfdGridFileBuilder)

def _registerNdfdGridManager(factory):
    from atmosci.ndfd.grid import NdfdGridFileManager
    factory._registerAccessManager('ndfd_grid', 'manage', NdfdGridFileManager)

def _registerNdfdGridReader(factory):
    from atmosci.ndfd.grid import NdfdGridFileReader
    factory._registerAccessManager('ndfd_grid', 'read', NdfdGridFileReader)



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class NdfdGridFactoryMethods(NDFDFactoryMethods):

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def buildForecastGridFile(self, forecast_date, variable, **kwargs):
        debug = kwargs.get('debug',False)
        region = kwargs.get('region',self.ndfd.grid.default_region)
        source = kwargs.get('source',self.ndfd.grid.default_source)
        timezone = kwargs.get('timezone',self.ndfd.grid.file_timezone)
        verbose = kwargs.get('verbose',debug)
        if verbose:
            print '\nbuildForecastGridFile :'
            print '     region :', region
            print '     source :', source
            print '   timezone :', timezone

        build_args = { 'debug':debug, 'source':source, 'verbose':verbose }
        builder = self.ndfdGridFileBuilder(forecast_date, variable, region, 
                                           timezone, None, None, **build_args)
        if verbose: print '\nbuilding grid file :', builder.filepath
        builder.close()

        # get lat, lon grids for the source/region couplet
        reader = self.staticFileReader(self.sourceConfig(source), region)
        lats = reader.getData('lat')
        lons = reader.getData('lon')
        reader.close()
        del reader

        # build the file
        builder.open('a')
        builder.build(lons=lons, lats=lats)
        builder.close()
        del lats, lons

        if debug:
            builder.open('r')
            time_attrs = builder.timeAttributes(self.datasetName(variable))
            builder.close()
            print '\nbuild file time attrs :'
            for key, value in time_attrs.items():
                print '    %s : %s' % (key, repr(value))

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def datasetName(self, variable, period='001-003'):
        return self.ndfd.variables[period][variable.lower()].grid_dataset

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def ndfdGridDirpath(self, fcast_date, variable, region, source, **kwargs):
        if self.project.get('shared_grid_dir', False):
            root_dir = self.sharedRootDir()
        else:
            root_dir = self.config.dirpaths.get('ndfd',
                            self.config.dirpaths.get('forecast',
                                 self.projectRootDir()))

        subdirs = kwargs.get('subdirs', self.ndfd.grid.subdirs)
        if isinstance(subdirs, tuple): subdirs = os.sep.join(subdirs)
        dirpath_template = os.path.join(root_dir, subdirs)

        source = kwargs.get('source', self.ndfd.grid.default_source)
        template_args = { 'month': fcast_date.strftime('%Y%m'),
                          'region': self.regionToDirpath(region),
                          'source': self.sourceToDirpath(source),
                          'year': fcast_date.year,
        }
        if variable != 'tempext':
            variable_config = self.variableConfig(variable)
            filetype = variable_config.get('grid_filetype', variable)
        else: filetype = 'tempext'

        if filetype == 'tempext':
            template_args['variable'] = self.config.filetypes.tempext.grid_path
        else: template_args['variable'] = variable

        grid_dirpath = dirpath_template % template_args

        if not os.path.exists(grid_dirpath):
            if kwargs.get('file_must_exist', False):
                errmsg = 'Reanalysis directory does not exist :\n%s'
                raise IOError, errmsg % grid_dirpath
            elif kwargs.get('make_grid_dirs', True):
                os.makedirs(grid_dirpath)
        return grid_dirpath

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def ndfdGridFilename(self, fcast_date, variable, region, source, **kwargs):
        debug = kwargs.get('debug',False)



        source = kwargs.get('source', self.ndfd.grid.default_source)
        if isinstance(source, basestring): source = self.sourceConfig(source)
        
        if debug:
            print '\ndfdGridFileBuilder :'
            print '    fcast_date :', fcast_date
            print '      variable :', variable
            print '        region :', region
            print '        source :', source
            print ' file template :', template

        template_args = { 'month' : fcast_date.strftime('%Y%m'),
                          'region': self.regionToFilepath(region),
                          'source': self.sourceToFilepath(source),
                          'year': fcast_date.year,
        }
       
        template = self.ndfd.grid.file_template
        return template % template_args

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def ndfdGridFilepath(self, fcast_date, variable, region, source, **kwargs):
        forecast_dir = \
            self.ndfdGridDirpath(fcast_date, variable, region, source, **kwargs)
        filename = \
            self.ndfdGridFilename(fcast_date, variable, region, source, **kwargs)
        return os.path.join(forecast_dir, filename)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def ndfdGridFileBuilder(self, fcast_date, variable, region, timezone,
                                  lons=None, lats=None, **kwargs):
        debug = kwargs.get('debug',False)
        verbose = kwargs.get('verbose',debug)
        if verbose:
            print '\ndfdGridFileBuilder :'
            print '   fcast_date :', fcast_date
            print '     variable :', variable
            print '       region :',  region
            print '     timezone :', timezone

        filepath = \
            self.ndfdGridFilepath(fcast_date, variable, region, **kwargs)
        start_time, end_time = self.monthTimespan(fcast_date, **kwargs)

        file_type = self.variableConfig(variable).grid_filetype
        Class = self.fileAccessorClass('ndfd_grid', 'build')
        return Class(filepath, self.config, file_type, region, start_time, 
                     end_time, timezone, lons, lats, **kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def ndfdGridFileManager(self, fcast_date, variable, region, mode='r',
                                  **kwargs):
        filepath = \
            self.ndfdGridFilepath(fcast_date, variable, region, **kwargs)

        Class = self.fileAccessorClass('ndfd_grid', 'manage')
        return Class(filepath, mode, **kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def ndfdGridFileReader(self, fcast_date, variable, region, **kwargs):
        filepath = \
            self.ndfdGridFilepath(fcast_date, variable, region, **kwargs)

        Class = self.fileAccessorClass('ndfd_grid', 'read')
        return Class(filepath, **kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def ndfdGridDatasetName(self, variable):
        return self.variableConfig(variable).grid_dataset

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _initNdfdGridFactory_(self, config_object, **kwargs):
        self._initNdfdFactory_(config_object, **kwargs)
        timezone = kwargs.get('grid_timezone', self.ndfd.grid.timezone)
        self.setFileTimezone(timezone)

        self.AccessRegistrars.ndfd_grid = {
                              'build': _registerNdfdGridBuilder,
                              'manage': _registerNdfdGridManager,
                              'read': _registerNdfdGridReader }

        self.completeInitialization(**kwargs)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class NdfdGridFileFactory(NdfdGridFactoryMethods, object):
    """
    Factory for managing data in NDFD forecast grid files.
    """
    def __init__(self, config_object=CONFIG, **kwargs):
        # initialize common configuration structure
        self._initFactoryConfig_(config_object, None, 'project')

        # initialize NDFD grid-specific configuration
        self._initNdfdGridFactory_(config_object, **kwargs)

        # simple hook for subclasses to initialize additonal attributes  
 

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# just-in-time registration of grid file access classes
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def _registerNdfdStaticBuilder(factory):
    from atmosci.ndfd.static import NdfdStaticGridFileBuilder
    factory.ndfd_builder = NdfdStaticGridFileBuilder


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class NdfdStaticFileFactory(SourceFileAccessorMethods, StaticFileAccessorMethods,
                            PathConstructionMethods, MinimalFactoryMethods, object):

    def __init__(self, config):
        if config is None:
            self.config = CONFIG.copy()
        else: self.config = config.copy()
        self.setProjectConfig('project')
        self.registry = None
        _registerNdfdStaticBuilder(self)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def ndfdStaticFilename(self, source, region, **kwargs):
        source_path = self.sourceToFilepath(source)
        template = self.getStaticFileTemplate(source)
        template_args = dict(kwargs)
        template_args['region'] = self.regionToFilepath(region, title=False)
        if isinstance(source, ConfigObject):
            config = self.getFiletypeConfig('static.%s' % source.name)
        else: config = self.getFiletypeConfig('static.%s' % source)
        if config is None: filetype = source_path
        else: filetype = config.get('type', source_path)
        template_args['type'] = filetype
        return template % template_args

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def ndfdStaticFilepath(self, source, region, **kwargs):
        static_dir = self.staticWorkingDir()
        filename = self.ndfdStaticFilename(source, region, **kwargs)
        return os.path.join(static_dir, filename)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def ndfdStaticFileBuilder(self, region, dataset_shape, **kwargs):
        debug = kwargs.get('debug',False)
        verbose = kwargs.get('verbose',debug)

        source = kwargs.get('source', self.config.static.ndfd)
        filepath = self.ndfdStaticFilepath(source, region, **kwargs)
        return self.ndfd_builder(filepath, self.config, region, dataset_shape, **kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def ndfdStaticFileReader(self, source, region, **kwargs):
        filepath = self.ndfdStaticFilepath(source, region, **kwargs)

        from atmosci.hdf5.grid import Hdf5GridFileReader
        return Hdf5GridFileReader(filepath)
        

