
import os
import datetime

from atmosci.utils.config import ConfigObject

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class SourceFileAccessorMethods:
    """ Methods for generating source directory and file paths plus
    instantiating the correct file accessor for a specific filetype /
    source / region / timespan combination.

    REQUIRED:
        1. must be included in a class derived from 
           atmosci.seasonal.factory.BaseProjectFactory

        2. subclass must also inherit from
           atmosci.seasonal.methods.paths.PathConstructionMethods
           and
           atmosci.seasonal.methods.access.BasicFileAccessMethods
           
        3. subclasses must register 'read', 'write' and 'build' accessor
           classes for the 'source' filetype using either:
              self._registerAccessManagers('source', ReaderClass,
                                           ManagerClass, BuilderClass)
              OR
              self._registerAccessManager('source', 'read', ReaderClass)
              self._registerAccessManager('source', 'manage', ManagerClass)
              self._registerAccessManager('source', 'build', BuilderClass)
    """

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # build source file
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def buildSourceFile(self, source_key, target_year, region_key, data_type,
                              **kwargs):
        verbose = kwargs.get('verbose', False)
        filetype = self.getFiletypeConfig('source')
        groups_in_file = filetype.groups
        region = self.getRegionConfig(region_key)
        source = self.getSourceConfig(source_key)

        # start and end dates for file
        start_date = kwargs.get("start_date", datetime.date(target_year,1,1))
        end_date = kwargs.get('end_date', datetime.date(target_year,12,31))

        # get latitude and longitude grids from the static file
        reader = self.getStaticFileReader(source, region)

        # create a build and initialize the file
        builder = self.getSourceFileBuilder(source, target_year, region,
                                            data_type, bbox=region.data)
        if verbose: print 'building', builder.filepath
        builder.build(True, True, reader.lons, reader.lats, **kwargs)
        builder.close()
        reader.close()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # source file access
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def sourceFileBuilder(self, source, target_year, region=None,
                                data_type=None, **kwargs):
        filepath = self.sourceGridFilepath(source, target_year, region,
                                           data_type, **kwargs)
        return self.newProjectFileBuilder(filepath, 'source', source, 
                                          target_year, region, **kwargs)
    
    getSourceFileBuilder = sourceFileBuilder

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def sourceFileManager(self, source, target_year, region=None, 
                                data_type=None, mode='r', **kwargs):
        filepath = self.sourceGridFilepath(source, target_year, region,
                                           data_type, **kwargs)
        return self.newProjectFileAccessor(filepath, 'manage', 'source', mode)
    getSourceFileManager = sourceFileManager

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def sourceFileReader(self, source, target_year, region=None, 
                               data_type=None, **kwargs):
        filepath = self.sourceGridFilepath(source, target_year, region,
                                           data_type, **kwargs)
        return self.newProjectFileAccessor(filepath, 'read', 'source')
    getSourceFileReader = sourceFileReader


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # source directory & file paths
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def sourceGridDir(self, source, region=None, data_type=None, **kwargs):
        shared = self.project.get('shared_source', False)
        if shared:
            source_dir = self.sharedRootDir('grid')
        else:
            source_dir = self.config.dirpaths.get('source', None)
            if source_dir is None:
                source_dir = self.projectRootDir()
            source_dir = os.path.join(source_dir, 'grid')
        if self.project.subproject_by_region:
            source_dir =  self.subdirByRegion(source_dir, region)
        source_dir = \
            os.path.join(source_dir, self.sourceToDirpath(source))
        if data_type is not None:
            source_dir = os.path.join(source_dir, data_type)
        if not os.path.exists(source_dir):
            if kwargs.get('dir_must_exist',kwargs.get('file_must_exist',False)):
                errmsg = 'Source grid directory does not exist :\n%s'
                raise IOError, errmsg % source_dir
            else: os.makedirs(source_dir)
        return source_dir

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def sourceGridFilename(self, source, source_year, region=None,
                                 data_type=None, **kwargs):
        template_args = dict(kwargs)
        template = None
        if data_type is not None:
            template_args['data_type'] = data_type
            template = self.getFilenameTemplate(data_type, None)
        if template is None:
            template = self.getFilenameTemplate(source)
        template_args['year'] = source_year
        template_args['source'] = self.sourceToFilepath(source)
        if region is not None:
            template_args['region'] = self.regionToFilepath(region, False)
        return template % template_args

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def sourceGridFilepath(self, source, source_year, region=None, 
                                 data_type=None, **kwargs):
        filepath = kwargs.get('filepath', None)
        if filepath is not None: return filepath
        source_dir = self.sourceGridDir(source, region, data_type)
        filename = self.sourceGridFilename(source, source_year, region,
                                           data_type, **kwargs)
        return os.path.join(source_dir, filename)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # source time coverage
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def earliestAvailableTime(self, source):
        earliest_time = source.get('earliest_available_time', 0)
        if isinstance(earliest_time, tuple):
            earliest_time = list(earliest_time)
        if isinstance(earliest_time, list):
            if len(earliest_time) == 1:
                earliest_time += [0,0]
            elif len(earliest_time) == 2:
                earliest_time.append(0)
            elif len(earliest_time) > 3:
                earliest_time = earliest_time[:3]
        else: earliest_time = [0,0,0]
        return datetime.time(*earliest_time)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def latestAvailableDate(self, source):
        latest_available_date = source.get('latest_available_date', None)
        if latest_available_date is None:
            latest_available_date = datetime.date.today()
        elif len(latest_available_date) == 2:
                year = datetime.date.today().year
                month, day = latest_available_date
                latest_available_date = datetime.date(year, month, day)
        elif len(latest_available_date) == 3:
                latest_available_date = datetime.date(*latest_available_date)
        else:
            errmsg = '"%s" is an invalid date specification'
            raise ValueError, errmsg % str(latest_available_date)

        days_behind = source.get('days_behind', None)
        if days_behind is not None:
            latest_available_date = \
                latest_available_date - datetime.timedelta(days=days_behind)
        return latest_available_date

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def latestAvailableTime(self, source, date=None):
        earliest_time = self.earliestAvailableTime(source)
        if date is not None:
            return datetime.datetime.combine(date, earliest_time)
        else:
            latest_date = self.latestAvailableDate(source)
            return datetime.datetime.combine(latest_date, earliest_time)

