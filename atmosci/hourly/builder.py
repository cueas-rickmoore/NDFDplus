
import datetime

import numpy as N
import h5py

from atmosci.utils import tzutils

from atmosci.hourly.grid import HourlyGridFileManager
from atmosci.seasonal.methods.builder import ProvenanceBuilderMethods


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class HourlyGridBuilderMethods(ProvenanceBuilderMethods):
    """ Methods that can be used to create a new HDF5 file with
    read/write access to 3D gridded data where the 3 dimensions are
    time(in hours), longitude and latitude. 1D and 2D datasets are
    also supported.

    - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    IMPORTANT INFORMATION FOR CREATING DERIVED CLASSES 
    - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    CLASS INITIALIZATION

    In order to properly function, classes that are derived from 
    HourlyGridFileBuildMethods and GridFileBuildMethods MUST call
    the following function in the precise order :
     
          self.preInitBuilder( ... )
          BASECLASS.__init__( ... )
          self.postInitBuilder(**kwargs)

    NOTE: See atmosci.seasonal.methods.builder.GridFileBuildMethods
          for arguments required by each them.

    The preInitBuilder method MUST be called by the __init__
    method of any subclass of builder prior to the call to the
    BASECLASS.__init__ method.
    
    A couple of dummy "hook" methods have been provided to ease
    the pain of subclassiing builders.
    
    The preferred way to implement additional functionality
    in preInitBuilder is to override the dummy hook method
    _additionalPreBuildInit implemented in
    atmosci.seasonal.methods.builder.GridFileBuildMethods
    It is passed all of the same arguments as __init__

    The postInitBuild method is also a dummy hook with no
    actual functionality, so feel free to do whatever you
    want in it. It is passed ONLY the __init__ arguments
    in **kwargs.

    IMPORTANT: If you find that you cannot get along without
    overriding the BASECLASS's preInitBuild method, you
    ABSOLUTELY MUST include the line:
             self._access_authority = ('r','a', 'w')
    If you don't the security measures inherited from the
    underlying file manager will prevent you from creating
    any files.

    - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    THE build METHOD
    - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    The BASECLASS.build() method builds everything in the file
    using information contained in your configuration file.

    However, each file may have attributes that the base build
    methods cannot anticipate, especially those associated
    with the file itself. The initFileAttributes method is
    called by build() to set the attributes of a file. 

    GridFileBuildMethods' implementation of initFileAttributes
    calls a dummy hook function named additionalFileAttributes.
    HourlyGridFileBuildMethods has implemented
    additionalFileAttributes in order to assure that a critical
    set of required time attributes gets set.  In order to make
    it easier for others to create sublcasses, that method
    also calls a dummy hook method named _initMyFileAttributes

    Both additionalFileAttributes and _setMyFileAttributes
    are passed arguments via the **kwargs dictionary passed
    to the build() method.

    See atmosci/seasonal/methods/builder.py for more info on
    the implementation, use and functionality of file builders.

    - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    EXAMPLE OF A MINIMAL SUBCLASS __init__ METHOD
    - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
     
    def __init__(self, hdf5_filepath, filetype, config_object,
                       start_time, timezone, region, source,
                       **kwargs):
     
        # REQUIRED: call BASECLASS' preInitBuilder
        self.preInitBuilder(config_object, filetype, start_time,
                            timezone, region, source,  **kwargs)

        # REQUIRED: call base class __init__ to create an instance
        BASECLASS.__init__(self, ..., **kwargs)
     
        # REQUIRED: initialize file attributes
        self.initFileAttributes(**kwargs)
        # file will be closed when initFileAttributes is done

        # OPTIONAL: call custom postInitBuilder for the drived class
        self.postInitBuilder(**kwargs)
     
        # re-open the file in append mode
        self.open(mode='a')
    """
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # properties - decorator access to protected "private" attributes
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    """
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
     region(self):
        return self.__region

    @property
    def source(self):
        return self.__source
    """

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def preInitBuilder(self, config, filetype, region, source, timezone,
                             **kwargs):
        """
        Set builder instance attributes that are required before
        BASCLASS__init__ is called.

        Arguments:
        ---------
        config: instance of atmosci.utis.confing.ConfigObject that
                has definitions of the data groups and data sets
                to be built.
        start_time: an instance of datetime.datetime describing the
                    first day and hour available in this file.
                    start_time MUST already be localized using
                    atmosci.tzutils.asHourInTimezone(). It does not 
                    have to be the same one as was passed in timezone.
        timezone: the timezone used for dates/times saved in this
                  file may either be a timezone object created using
                  atmosci.tzutils.asTimezoneObj(), or a string
                  containing a compatible name for the timezone.
        regions: the name of region covered by yhe file. 

        IMPORTANT: either end_time or num_hours must be specified
                   in kwargs
            end_time: an instance of datetime.datetime describing
                      the last day and hour available in this file.
                      end_time MUST already be localized using
                      atmosci.tzutils.asHourInTimezone(). It does
                      not have to be the same one as was passed
                      in timezone.
            num_hours: total number of hours in the file as an integer.
        
        NOTE: if end_time is passed, num_hours will be calculated as the
              difference between end_time and start_time.
              if num_hours is passed, end_time will be set to num_hours
              after start_time. e.g. 2017-01-01:01 and num_bours = 10
              will set end_time to 2017-01-01:10
        """
        self._preInitHourlyFileBuilder_(timezone, **kwargs)
        ProvenanceBuilderMethods.preInitBuilder(self, config, filetype,
                                 source, region, **kwargs)
    
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def additionalFileAttributes(self, **kwargs):
        """
        Returns timezone, start_time & end_time attributes based on values
        specified in preInitBuilder().
        Also adds any attributes returned from a subclass' implementation
        or _initMyFileAttributes()
        """
        attr_dict = { 'end_time':tzutils.hourAsString(self.end_time),
                      'num_hours':self.num_hours,
                      'start_time':tzutils.hourAsString(self.start_time),
                      'timezone':self.timezone,
                    }
        return attr_dict
    
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def generateEmptyProvenance(self, prov_config, attrs):
        start_time = attrs.get('start_time', self.start_time) 
        end_time = attrs.get('end_time', self.end_time)
        timezone = prov_config.get('timezone',
                               attrs.get('timezone', self.default_timezone))
        hour = tzutils.asHourInTimezone(start_time, timezone)
        end_time = tzutils.asHourInTimezone(end_time, timezone)
        time_increment = \
            datetime.timedelta(hours=prov_config.get('frequency',1))


        records = [ ]
        record_tail = prov_config.empty[1:]
        while hour <= end_time:
            record = (tzutils.hourAsString(hour),) + record_tail
            records.append(record)
            hour += time_increment
        return records

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # hook function implementationss
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _additionalPreBuildInit(self, **kwargs):
        times_missing = 'Neither "start_time" nor "end_time" were found in'
        times_missing += ' **kwargs. One of them MUST be specified.'
        hours_mising = '"num_hours" must be passed via **kwargs when only' 
        hours_mising = ' "%s" is specified' 

        start_time = kwargs.get('start_time', None)
        if start_time is None:
            end_time = kwargs.get('end_time', None)
            assert(end_time is not None), times_missing
            end_time = tzutils.asHourInTimezone(end_time, self.tzinfo)
            num_hours = kwargs.get('num_hours', None)
            assert(num_hours is not None), hours_missing % 'end_time'
            start_time = end_time - datetime.timedelta(hours=num_hours)
        else:
            start_time = tzutils.asHourInTimezone(start_time, self.tzinfo)
            end_time = kwargs.get('end_time', None)
            if end_time is None:
                num_hours = kwargs.get('num_hours', None)
                assert(num_hours is not None), hours_missing % 'start_time'
                end_time = start_time + datetime.timedelta(hours=num_hours)
            else:
                end_time = tzutils.asHourInTimezone(end_time, self.tzinfo)

        self.start_time = start_time
        self.end_time = end_time
        self.num_hours = tzutils.hoursInTimespan(start_time, end_time)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _shapeForDataset(self, dataset):
        source_dimensions = self._sourceDimensions()
        view = self._mappedDatasetView(dataset)

        shape = [ ]
        for dim in dataset.view:
            if isinstance(dim, basestring):
                if dim == 'time':
                    period = dataset.get('period','hour')
                    if period in ('hour','hours'):
                        frequency = dataset.get('frequency',1)
                        if frequency == 1: shape.append(self.num_hours)
                        else: shape.append((self.num_hours/frequency) + 1)
                    else:
                        errmsg = '"%s" is an unsupported time period.'
                        raise ValueError, errmsg % period
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

    def _resolveHourlyAttributes(self, object_config, **kwargs):
        attrs = {
          'period':kwargs.get('period',object_config.get('period','hour')),
        }
        frequency = kwargs.get('frequency',object_config.get('frequency',1))
        attrs['frequency'] = frequency
        # dataset timezone attribute must be a string
        timezone = str(kwargs.get('timezone',
                       object_config.get('timezone', self.default_timezone)))
        attrs['timezone'] = timezone

        # add object's time span
        start_time = kwargs.get('start_time', self.start_time)
        start_time = tzutils.asHourInTimezone(start_time, timezone)
        attrs['start_time'] = tzutils.hourAsString(start_time)

        end_time = kwargs.get('end_time', self.end_time)
        end_time = tzutils.asHourInTimezone(end_time, timezone)
        attrs['end_time'] = tzutils.hourAsString(end_time)

        num_hours = tzutils.hoursInTimespan(start_time, end_time)
        attrs['num_hours'] = (num_hours / frequency) + 1
        
        # object may or may not have a reference time
        reference_time = kwargs.get('reference_time', None)
        if reference_time is not None: # and reference_time != start_time:
            reference_time = tzutils.asHourInTimezone(reference_time, timezone)
            attrs['reference_time'] = tzutils.hourAsString(reference_time)

        return attrs

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _resolveProvenanceBuildAttributes(self, prov_config, **kwargs):
        attrs = self._resolveCommonAttributes(**kwargs)
        attrs['description'] = prov_config.get('description', None)
        attrs['key'] = prov_config.name
        view = prov_config.names[0]
        attrs.update(self._resolveScopeAttributes(prov_config, **kwargs))
        attrs.update(self._resolveTimeAttributes(prov_config, **kwargs))
        attrs['generator'] = \
            kwargs.get('generator', prov_config.get('generator', attrs['key']))
        return attrs

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _resolveScopeAttributes(self, object_config, **kwargs):
        attrs = { }
        scope = kwargs.get('scope', object_config.get('scope',None))
        if scope is not None:
            attrs['scope'] = scope
            if scope == 'time':
                perd = kwargs.get('period',object_config.get('period','hour'))
            else: perd = kwargs.get('period',object_config.get('period',None))
            if perd is not None:
                attrs['period'] = perd
        return attrs
 
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _resolveSourceAttributes(self, **kwargs):
        attrs = { }
        source = self.source
        attrs['source'] = kwargs.get('source_tag',
                                 source.get('tag', source.name))
        # check for one of NRCC's ACIS grids
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
        scope = kwargs.get('scope', object_config.get('scope', None))
        # cannot be a time dataset without scope == time
        if scope == 'time':
            return self._resolveHourlyAttributes(object_config, **kwargs)
        else:
            period = kwargs.get('period',object_config.get('period',None))
            if period == 'date':
                return self._resolveDateAttributes(dataset_config, **kwargs)
            elif period == 'doy':
                return self._resolveDoyAttributes(dataset_config, **kwargs)
        return { }

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

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _preInitHourlyFileBuilder_(self, timezone, **kwargs):
        self._preInitHourlyFileManager_(**kwargs)
        if tzutils.isValidTimezoneObj(timezone):
            self.timezone = tzutils.timezoneAsString(timezone)
            self.tzinfo = timezone
        else:
            self.timezone = timezone
            self.tzinfo = tzutils.asTimezoneObj(timezone)
        self.default_timezone = self.timezone


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class HourlyGridFileBuilder(HourlyGridBuilderMethods, HourlyGridFileManager):
    """ Creates a new HDF5 file with read/write access to 3D gridded data
    where the first dimension is time, the 2nd dimension is longitude and
    the 3rd dimension is latitude.
    """
    def __init__(self, hdf5_filepath, config, filetype, region, source,
                       timezone, lons=None, lats=None, **kwargs):
        self.preInitBuilder(config, filetype, region, source, timezone,
                            **kwargs)
        mode = kwargs.get('mode', 'w')
        if mode == 'w':
            self.load_manager_attrs = False
        else: self.load_manager_attrs = True

        HourlyGridFileManager.__init__(self, hdf5_filepath, mode)
        # set the time span for this file
        #self.initTimeAttributes(**kwargs)
        # close the file to make sure attributes are saved
        self.close()
        # reopen the file in append mode
        self.open(mode='a')
        # build lat/lon datasets if they were passed
        if lons is not None:
            self.initLonLatData(lons, lats, **kwargs)

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _loadManagerAttributes_(self):
        if self.load_manager_attrs:
            HourlyGridFileManager._loadManagerAttributes_(self)
        else:
            self._loadProvenanceGenerators_()
            self.time_attr_cache = { }
