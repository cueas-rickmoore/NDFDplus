
import os

from copy import deepcopy
import datetime

from atmosci.utils.config import ConfigObject
from atmosci.utils.timeutils import timeSpanToIntervals, yearFromDate

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# CONVENIENCE FUNCTIONS COMMON TO MULTIPLE PROJECT TYPES
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class MinimalFactoryMethods:

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def completeInitialization(self, **kwargs):
        """ Pass thru so derived classes can assert minor initialization
        requirements.
        """
        pass

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # configuration access functions 
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def fromConfig(self, config_item_path, **kwargs):
        try:
            item = self.config.get(config_item_path)
        except KeyError as e:
            if 'default' in kwargs: return kwargs['default']
            errmsg = 'full path = "%s"' % config_object_path
            e.args += (errmsg,)
            raise e
        if isinstance(item, ConfigObject): return item.copy()
        else: return deepcopy(item)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def datasetConfig(self, dataset_key):
        return self.config.datasets[dataset_key]
    getDatasetConfig = datasetConfig # backwards compatibility

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def filetypeConfig(self, filetype_key):
        if '.' in filetype_key:
            filetype, source_key = filetype_key.split('.')
            ft_cfg = self.config.filetypes.get(filetype, None)
            if ft_cfg is not None:
                src_cfg = ft_cfg.get(source_key, None)
                if src_cfg is not None: return src_cfg
        else:
            ft_cfg = self.config.filetypes.get(filetype_key, None)
        return ft_cfg
    getFiletypeConfig = filetypeConfig # backwards compatibility

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def groupConfig(self, group_key):
        return self.config.groups[group_key]
    getGroupConfig = groupConfig # backwards compatibility

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def projectConfig(self):
        return self.config.project
    getProjectConfig = projectConfig # backwards compatibility

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def provenanceConfig(self, provenance_key):
        return self.config.provenance[provenance_key]
    getProvenanceConfig = provenanceConfig # backwards compatibility

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def regionConfig(self, region_key=None):
        key = region_key
        if key is None: key = self.config.project.get('region', None)
        if key is not None:
            region = self.config.regions.get(key, None)
            if region is not None: return region
            errmsg = "%s does not correspond to any configured region."
            raise KeyError, errmsg % key
        else:
            errmsg = "Region key was not passed to function and no default" 
            errmsg = "%s was found in the configuration object." % errmsg
            raise ValueError, errmsg
    getRegionConfig = regionConfig # backwards compatibility

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def registryConfig(self): return self.registry
    getRegistryConfig = registryConfig # backwards compatibility

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def setProjectConfig(self, project='project'):
        errmsg = None
        if project == 'project':
            proj_obj = self.config.get('project', None)
            if proj_obj is None:
                errmsg = '"self.config.project is not defined.'
            else: self.project = proj_obj
        elif isinstance(project, ConfigObject):
            self.project = project.copy('project',None)
        elif isinstance(project, basestring):
            if projects in self.config:
                proj_obj = self.config.projects.get(project, None)
                if proj_obj is None:
                    errmsg = 'self.config.projects.%s is not defined'
                    errmsg = errmsg % project
                else: self.project = proj_obj
            else:
                errmsg = 'self.config.projects is not defined'
        else:
            errmsg = '%s is an invalid type for the "project" argument'
            raise TypeError, errmsg % str(type(project))
        
        if errmsg is not None:
            raise LookupError, errmsg

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    
    def sourceConfig(self, source_key=None):
        key = source_key
        if key is None: key = self.config.project.get('source', None)
        if key is not None:
            source = self.config.sources.get(key, None)
            if source is not None: return source
            errmsg = "%s does not correspond to any configured source."
            raise KeyError, errmsg % key
        else:
            errmsg = "Source key was not passed to function and no default" 
            errmsg = "%s was found in the project configuration." % errmsg
            raise ValueError, errmsg
    getSourceConfig = sourceConfig # backwards compatibility

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def sourceName(self, source):
        if isinstance(source, ConfigObject):
            return source.name
        elif isinstance(source, basestring):
            return source.lower()
        else:
            errmsg = 'Unsupported type for "source" argument : %s'
            return TypeError, errmsg % str(type(source))

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def timestamp(self, as_file_path=False):
        if as_file_path:
            return datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
        else: return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def useDirpathsForMode(self, mode='default'):
        """
        Set the directory paths for this instance to those used by the
        project mode passed. If the mode argument is not passed, the
        default directory paths for the project will be used.
        """
        mode_path = 'modes.%s' % mode
        mode_cfg = self.config.get(mode_path, None)
        if mode_cfg is None:
            errmsg = '"self.config.modes.%s" is not defined.'
            raise KeyError, errmsg % mode

        dirpaths = mode_cfg.get('dirpaths', None)
        if dirpaths is None:
            errmsg = '"self.config.modes.%s.dirpaths" is not configured.'
            raise KeyError, errmsg % mode
        
        self.config.dirpaths.update(dirpaths)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _registerAccessClasses(self):
        raise NotImplementedError

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _registerAccessManager(self, file_type, mgr_type, accessor_class):
        accessor = '%s.%s' % (file_type, mgr_type)
        self.AccessClasses[accessor] = accessor_class

    def _registerAccessManagers(self, file_type, reader, manager, builder):
        self.AccessClasses[file_type] = \
             { 'read':reader, 'manage':manager, 'build':builder }

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _initFileManagerClasses_(self):
        # create a dictionary for registration of file access classes
        if not hasattr(self, 'AccessClasses'):
            self.AccessClasses = ConfigObject('AccessClasses', None)
        # register the factory-specific accessors
        self._registerAccessClasses()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _initFactoryConfig_(self, config_object, registry_object=None,
                                  project=None):
        self.config = config_object.copy()

        if project is None:
            self.setProjectConfig('project')
        else: self.setProjectConfig(project)

        if registry_object is not None:
            self.registry = registry_object.copy()
        else: self.registry = None

        # initilaize file access managers
        self._initFileManagerClasses_()



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class BasicProjectFactoryMethods(MinimalFactoryMethods):

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # date and time span utilities
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def dateInSeason(self, date):
        year = self.targetYearFromDate(date)
        start_date = self.seasonStartDate(year)
        end_date = self.seasonEndDate(year)
        return (date >= start_date and date <= end_date)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def listDatesBetween(self, start_date, end_date):
        if end_date is None: return (start_date,)
        else: return timeSpanToIntervals(start_date, end_date, 'day', 1)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def seasonConfig(self, filetype=None):
        if filetype is None: return self.config.project
        else: 
            return self.config.filetypes[filetype].get('season',
                                                       self.config.project)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def seasonEndDate(self, target_year, filetype=None):
        season = self.seasonConfig(filetype)
        # end date is always in the target year
        return datetime.date(target_year, *season.end_day)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def seasonStartDate(self, target_year, filetype=None):
        # end day is always in the target year
        season = self.seasonConfig(filetype)
        # if start month is later in the year than to end month,
        # start date is in the year previous to the target year
        if season.start_day[0] > season.end_day[0]:
            return datetime.date(target_year-1, *season.start_day)
        # start day and end day are both in the same year
        else: return datetime.date(target_year, *season.start_day)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def targetYearFromDate(self, date, filetype=None):
        season = self.seasonConfig(filetype)
        if date.month > season.end_day[0]: return date.year + 1
        else: return date.year

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def targetDateSpan(self, year_or_date, filetpye=None):
        if isinstance(year_or_date, int):
            # input year is assumed to be the target year
            target_year = year_or_date

        elif isinstance(year_or_date, (tuple,list)):
            # input is a date tuple (year, month, day)
            target_year = \
                self.targetYearFromDate(datetime.date(year_or_date), filetype)

        elif isinstance(year_or_date, (datetime.datetime,datetime.date)):
            # year_or_start_date == datetime instance
            target_year = self.targetYearFromDate(year_or_date, filetype)

        else:
            errmsg = "Invalid type for 'year_or_date' argument : %s"
            raise TypeError, errmsg % type(year_or_date)

        end_date = self.seasonEndDate(target_year)
        start_date = self.seasonStartDate(target_year)
        return start_date, end_date


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class BaseProjectFactory(BasicProjectFactoryMethods, object):
    """ Base class containing functions common to all factory subclasses.
    """

    def __init__(self, config_object, registry_object=None, project=None,
                       **kwargs):
        self._initFactoryConfig_(config_object, registry_object, project)

        # allow override of targetYearFromDate() from project's config object
        tyFunction = config_object.get('targetYearFromDate', None)
        if tyFunction is not None:
            self.targetYearFromDate = tyFunction

        # simple hook for subclasses to initialize additonal attributes  
        self.completeInitialization(**kwargs)

