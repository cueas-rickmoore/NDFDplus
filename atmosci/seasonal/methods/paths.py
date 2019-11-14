
import os
import datetime

from atmosci.utils.config import ConfigObject

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# CONVENIENCE FUNCTIONS COMMON TO MULTIPLE PROJECT TYPES
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class PathConstructionMethods:
    """ Methods common to all factory subclasses.

    REQUIRED:
        1. must be included in a class derived from 
           atmosci.seasonal.factory.BaseProjectFactory
    """

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # log file directory and file path
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def logFileDir(self):
        log_dir = os.path.join(self.projectRootDir(), 'logs')
        if not os.path.exists(log_dir): os.makedirs(log_dir)
        return log_dir

    def logFilepath(self, process_key, pid=None):
        time_str = self.timestamp().replace(' ','-').replace(':','')
        if pid is not None:
            filename = '%s-%s-%s.log' % (time_str, process_key, str(pid))
        else:
            filename = '%s-%s.log' % (time_str, process_key)
        return os.path.join(self.logFileDir(), log_filename)


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # common project directory path generators
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def projectRootDir(self, **kwargs):
        root_dir = self.config.dirpaths.get('project', None)
        if root_dir is None:
            if self.project is None: root = None
            else: root = self.project.get('root', None)
            if root is None: return self.workingDir()
            root_dir = os.path.join(self.workingDir(),
                                    self.normalizeDirpath(root))
        if not os.path.exists(root_dir): 
            if kwargs.get('dir_must_exist',kwargs.get('file_must_exist',False)):
                errmsg = 'Project root directory does not exist :\n%s'
                raise IOError, errmsg % root_dir
            else: os.makedirs(root_dir)
        return root_dir

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def projectGridDir(self, source, region=None, **kwargs):
        project_dir = self.projectRootDir()
        if self.project.subproject_by_region:
            project_dir =  self.subdirByRegion(project_dir, region)
        project_dir = os.path.join(project_dir, self.sourceToDirpath(source))
        if not os.path.exists(project_dir):
            if kwargs.get('dir_must_exist',kwargs.get('file_must_exist',False)):
                errmsg = 'Project grid directory does not exist :\n%s'
                raise IOError, errmsg % project_dir
            else: os.makedirs(project_dir)
        return project_dir

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def sourceDirpath(self, source, region):
        order = self.project.get('source_subdir_order', 'region-source')
        if order == 'region-source':
            dirpath = os.path.join(self.projectRootDir(),
                                   self.regionToDirpath(region),
                                   self.sourceToDirpath(source))
        else:
            dirpath = os.path.join(self.projectRootDir(),
                                   self.sourceToDirpath(source),
                                   self.regionToDirpath(region))
        if not os.path.exists(dirpath): os.makedirs(dirpath)
        return dirpath

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def appDataRootDir(self, **kwargs):
        appdata_dir = self.config.dirpaths.get('app_data', None)
        if appdata_dir is None:
            data_dir = self.dataRootDir(**kwargs)
            appdata_dir = os.path.join(data_dir, 'app_data')
        if not os.path.exists(appdata_dir):
            if kwargs.get('dir_must_exist',kwargs.get('file_must_exist',False)):
                errmsg = 'App data directory does not exist :\n%s'
                raise IOError, errmsg % appdata_dir
            else: os.makedirs(appdata_dir)
        return appdata_dir

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def dataRootDir(self, **kwargs):
        data_dir = self.config.dirpaths.get('data', None)
        if data_dir is None:
            data_dir = os.path.join(self.workingDir(), 'data')
        if not os.path.exists(data_dir):
            if kwargs.get('dir_must_exist',kwargs.get('file_must_exist',False)):
                errmsg = 'Data directory does not exist :\n%s'
                raise IOError, errmsg % data_dir
            else: os.makedirs(data_dir)
        return data_dir

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def sharedRootDir(self, data_format=None, **kwargs):
        shared_dir = self.config.dirpaths.get('shared', None)
        if shared_dir is None:
            shared_dir = os.path.join(self.workingDir(), 'shared')
        if data_format is not None:
            shared_dir = os.path.join(shared_dir, data_format)
        if not os.path.exists(shared_dir):
            if kwargs.get('dir_must_exist',kwargs.get('file_must_exist',False)):
                errmsg = 'Shared root directory does not exist :\n%s'
                raise IOError, errmsg % shared_dir
            else: os.makedirs(shared_dir)
        return shared_dir

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def subdirByRegion(self, root_dir, region=None):
        if region is None:
            region_dir = self.regionToDirpath(self.project.region)
        else:
            region_dir = self.regionToDirpath(region)
        subdir = os.path.join(root_dir, region_dir)
        if not os.path.exists(subdir): os.makedirs(subdir)
        return subdir
    
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def subdirPath(self, subdir_key):
        subdir_path = self.config.subdir_paths.get(subdir_key, None)
        if subdir_path is None:
            errmsg = '"%s" not found in "self.config.subdir_paths"'
            raise KeyError, errmsg % subdir_key

        if isinstance(subdirs, tuple): return os.sep.join(subdir_path)
        return subdir_path 

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def targetGridDir(self, target_year, source, region=None, **kwargs):
        project_dir = self.projectGridDir(source, region)
        target_dir = os.path.join(project_dir, str(target_year))
        if not os.path.exists(target_dir):
            if kwargs.get('dir_must_exist',kwargs.get('file_must_exist',False)):
                errmsg = 'Target year directory does not exist :\n%s'
                raise IOError, errmsg % target_dir
            else: os.makedirs(target_dir)
        return target_dir

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def workingDir(self, **kwargs):
        working_dir = self.config.get('dirpaths.working',
                                      self.config.get('working_dir', None))
        if working_dir is None:
            err_msg = 'Configuration contains no working directory path.'
            raise LookupError, err_msg

        if not os.path.exists(working_dir):
            if kwargs.get('dir_must_exist',kwargs.get('file_must_exist',False)):
                errmsg = 'Working directory does not exist :\n%s'
                raise IOError, errmsg % working_dir
            else: os.makedirs(working_dir)
        return working_dir

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # project directory & data file path
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def projectGridFilename(self, filetype, source, region=None,
                                  target_year=None, **kwargs):
        template = self.getFilenameTemplate(filetype)
        template_args = templateArgs(target_year, source, region)
        if not 'region' in template_args:
            project_region = self.config.project.region
            template_args['region'] = self.regionToFilepath(project_region)
        template_args.update(dict(kwargs))
        return template % template_args

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def projectGridFilepath(self, filetype, source, region=None, 
                                  target_year=None, **kwargs):
        if target_year is not None:
            target_dir = self.targetGridDir(target_year, source, region)
        else:
            target_dir = self.projectGridDir(source, region)
        filename = \
        self.projectGridFilename(filetype, source, region, target_year,
                                 **kwargs)
        return os.path.join(target_dir, filename)


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # miscellaneous directory and file path  utilities
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _findFilenameTemplate(self, cfgobj):
        # check to see if it has a custom template 
        template = cfgobj.get('template', None)
        if template is not None: return template
        # no template, maybe there is the a filename template key
        return cfgobj.get('filename', None)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def getDownloadFileTemplate(self, source):
        template = None
        # check source config object for custom template
        if isinstance(source, ConfigObject):
            description = source.description
            source_key = source.name
            # does source have a custom dowonload template
            template = source.get('download_template', None)
            if template is not None: return template
            # does source use filenames based on type of download file
            filetype = source.get('download_filetype', None)
            if filetype is not None:
                template = self.config.filenames.get(filetype, None)
                if template is not None: return template
        else: # source is not a config object
            description = None
            # key is just the name of the data source
            source_key = source
        # no custom template, search global filename templates for source key
        template = self.config.filenames.get(source_key, None)
        if template is not None: return template
        # still no template, try the generic template for download files
        template = self.config.filenames.get('download', None)
        if template is not None: return template
        # could not find any template for download files
        if description is None:
            errmsg = 'Unable to locate dwonload filename template for %s'
            raise KeyError, errmsg % source_key
        else:
            errmsg = 'Unable to locate download filename template for %s("%s")'
            raise KeyError, errmsg % (source_key, description)
    downloadFileTemplate = getDownloadFileTemplate

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def filenameTemplate(self, filetype, default=None):
        if isinstance(filetype, ConfigObject):
            # any config object might have a custom template
            template = self._findFilenameTemplate(filetype)
            if template is not None: return template
            # no custom template
            if filetype.parent.name == 'sources':
                # all source files default to the same template
                tmpl_key = 'source'
            # use the object's name as the template key
            else: tmpl_key = filetype.name
        # assume filetype is a string that contains the template key
        else: tmpl_key = filetype
        # look up the template and return it
        return self.config.filenames.get(tmpl_key, default)
    getFilenameTemplate = filenameTemplate

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def gridNodeToPath(self, node, precision=3):
        factor = pow(10., precision)
        if node[0] < 0: lon, lat = node # longitudes are always negative
        else: lat, lon = node
        nlon, nlat = self.acisNodeFinder(lon, lat, precision)
        return '%d-%d' % (int(abs(nlon) * factor), int(nlat * factor))
    gridNodeToDirpath = gridNodeToPath
    gridNodeToFilename = gridNodeToPath

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def normalizeDirpath(self, obj_or_string):
        if isinstance(obj_or_string, basestring): name = obj_or_string
        else: name = obj_or_string.get('subdir', obj_or_string.name)
        _name = name.replace(' ','_').replace('-','_').replace('.',os.sep)
        return os.path.normpath(_name)

    def normalizeFilepath(self, obj_or_string):
        if isinstance(obj_or_string, basestring): name = obj_or_string
        else: name = obj_or_string.get('tag', obj_or_string.name)
        _name = name.replace('_',' ').replace('-',' ').replace('.',' ')
        return _name.title().replace(' ','-')

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def regionToDirpath(self, region):
        if isinstance(region, ConfigObject):
            path = region.get('tag', None)
            if path is not None: return path
            path = region.name
        else: path = region
        if len(path) in (1, 2): return path.upper()
        else: return self.normalizeDirpath(path)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def regionToFilepath(self, region, title=True):
        if isinstance(region, ConfigObject):
            path = region.get('tag', None)
            if path is not None: return path
            path = region.name
        else: path = region
        if len(path) in (1, 2): return path.upper()
        if title:
            path = path.replace('_',' ').replace('.',' ').title()
            return path.replace(' ','-')
        else: return path.replace('_','-').replace('.','-')

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def sourceToDirpath(self, source):
        if isinstance(source, ConfigObject):
            subdir = source.get('subdir', None)
            if subdir is not None:
                return self.normalizeDirpath(subdir)
            else: return self.normalizeDirpath(source.name.lower())
        return self.normalizeDirpath(source.lower())

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def sourceToFilepath(self, source):
        if isinstance(source, ConfigObject):
            path = source.get('tag', None)
            if path is None: path = source.name.upper()
        else: path = source.upper()
        return path.replace(' ','-').replace('_','-').replace('.','-')

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def templateArgs(self, target_year=None, source=None, region=None):
        template_args =  { }
        if target_year is not None: template_args['year'] = target_year
        if source is not None:
             template_args['source'] = self.sourceToFilepath(source)
        if region is not None:
            template_args['region'] = self.regionToFilepath(region)
        return template_args

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def timeToDirpath(self, time_obj):
        if isinstance(time_obj, datetime.datetime):
            return time_obj.strftime('%Y%m%d%H%M')
        elif isinstance(time_obj, datetime.date):
            return time_obj.strftime('%Y%m%d')
        else: return time_obj

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def timeToFilepath(self, time_obj):
        if isinstance(time_obj, datetime.datetime):
            return time_obj.strftime('%Y-%m-%d-%H%M')
        elif isinstance(time_obj, datetime.date):
            return time_obj.strftime('%Y-%m-%d')
        else: return time_obj

