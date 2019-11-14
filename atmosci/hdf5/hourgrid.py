""" Classes for accessing hourly data from Hdf5 encoded grid files.
"""

import datetime

import numpy as N

from atmosci.utils import tzutils

from atmosci.hdf5.grid import Hdf5GridFileReader, Hdf5GridFileManager

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from atmosci.hdf5.config import DEFAULT_TIME_ZONE
DEFAULT_TZINFO = tzutils.asTimezoneObj(DEFAULT_TIME_ZONE)

ARG_TYPE_ERROR = '"%s" is an invalid type for "%s" argument.'
TIME_SPAN_ERROR = 'Invalid time span. Either "hour" OR '
TIME_SPAN_ERROR += '"start_time" plus "end_time" are required.'
TIME_ZONE_ERROR = 'is not a pytz-compatible time zone description.'

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

compareValues = { '==': lambda v1,v2 : v1 == v2,
                  '<':  lambda v1,v2 : v1 < v2,
                  '<=': lambda v1,v2 : v1 <= v2,
                  '>':  lambda v1,v2 : v1 > v2,
                  '>=': lambda v1,v2 : v1 >= v2,
                  '!=': lambda v1,v2 : v1 != v2 }

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# timezone-specific file management methods
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class TimeZoneManagementMethods:

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def asDatasetTime(self, dataset_path, hour, timezone=None):
        """
        Convert an hour from it's local timezone to the
        corresponding hour in the timezone used by the
        referenced dataset.

        Arguments:
        ---------
            dataset_path: full dot.path to dataset (string)
            hour: a datetime.datetime object or string 'YY-MM-DD:HH'

        Optional:
        --------
            timezone: the local timezone for the hour. USED ONLY 
                      WHEN hour DOES NOT HAVE A VALID tzinfo
                      ATTRIBUTE. Default value is None.
                The value of timezone may be a string containing
                a valid timezone name or a tzinfo object created
                using atmosci.tzutils.asTimezoneObj()
                
        NOTE: When hour does not have a valid tzinfo attribute
              and the value of timezone is None, the timezone in
              the Reader/Manager's default_timezone attribute
              will be used as the hour's local timezone. 

        Returns:
        -------
            datetime.datetime object corrected to the timezone
            required to access data in the dataset.
        """
        ds_timezone = self.datasetAttribute(dataset_path, 'timezone', None)
        if ds_timezone is None: return self.asFileTime(hour, timezone)
        ds_tz = tzutils.asTimezoneObj(ds_timezone)

        local_hour = self.asLocalTime(hour, timezone)
        return tzutils.asHourInTimezone(hour, ds_tz)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def asDefaultTime(self, hour):
        """
        Assign an hour to the timezone in the default_timezone
        attribute of the current instance of the Reader/Manager.
        The default is "US.Eastern" but may be overriden using
        the setDefaultTimezone() method.

        Arguments:
        ---------
            hour: a datetime.datetime object or string like 'YY-MM-DD:HH'

        Returns:
        -------
            datetime.datetime object corrected to the default timezone 
        """
        return tzutils.asHourInTimezone(hour, self.default_timezone)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def asFileTime(self, hour, timezone=None):
        """
        Convert an hour from it's local timezone to the
        corresponding hour in the timezone specified in
        the file's timezone attribute.

        Argument:
        --------
            hour: a datetime.datetime object or string 'YY-MM-DD:HH'

        Optional:
        --------
            timezone: the local timezone for the hour. USED ONLY 
                      WHEN hour DOES NOT HAVE A VALID tzinfo
                      ATTRIBUTE. Default value is None.
                The value of timezone may be a string containing
                a valid timezone name or a tzinfo object created
                using atmosci.tzutils.TimezoneObj()
                
        NOTE: When hour does not have a valid tzinfo attribute
              and the value of timezone is None, the timezone in
              the Reader/Manager's default_timezone attribute
              will be used as the hour's local timezone. 

        Returns:
        -------
            datetime.datetime object corrected to the file's
            default timezone.
        """
        local_hour = self.asLocalTime(hour, timezone)
        return tzutils.asHourInTimezone(local_hour, self.tzinfo)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def asLocalTime(self, hour, timezone=None):
        if timezone is None: return self.asDefaultTime(hour)
        return tzutils.asLocalTime(hour, timezone)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def isValidTimeString(self, time_string, include_timeszone=False):
        return tzutils.isValidHourString(time_string, include_timeszone) 


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# hourly file management methods
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class Hdf5HourlyGridReaderMethods(TimeZoneManagementMethods):

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def areaSlice(self, dataset_path, min_lon, max_lon, min_lat, max_lat,
                        **kwargs):
        """
        Extracts a subset of data for all hours at nodes within the
        area described by the lon/lat bounding box.

        Arguments:
        ---------
            dataset_path: full dot.path to dataset (string)
            min_lon: minimum longitude to include (float)
            max_lon: maximum longitude to include (float)
            min_lat: minimum latitude to include (float)
            max_lat: maximum latitude to include (float)

        Returns:
        -------
            NumPy array containing the retrieved data.
        """
        dataset = self.getDataset(dataset_path)

        min_y, min_x = self.ll2index(min_lon, min_lat)
        max_y, max_x = self.ll2index(max_lon, max_lat)

        data = self._areaSlice(dataset, min_y, min_x, max_y, max_x, **kwargs)
        return self._processDataOut(dataset_path, data, **kwargs)

    get2Dslice = areaSlice

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def asObjectTime(self, time_obj):
        pass

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def compareTimes(self, time1, op, time2):
        if isinstance(time1, datetime.datetime) \
        and isinstance(time2, datetime.datetime):
            return compareValues[op](time1, time2)
        else:
            errmsg = 'Invalid type for hour arguments : %s'
            raise TypeError, errmsg % str(type(hour1))

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def dataAtNode(self, dataset_path, lon, lat, **kwargs):
        """
        Extracts data for all hours at the single node that is
        closest to the coordinates requested.

        Arguments:
        ---------
            dataset_path: full dot.path to dataset (string)
            lon: longitude for grid node (float)
            lat: latitude for grid node (float)

        Returns:
        -------
            NumPy array containing the retrieved data.
        """
        y, x = self.ll2index(lon, lat)
        dataset = self.getDataset(dataset_path)
        data = self._dataAtNode(dataset, y, x)
        return self._processDataOut(dataset_path, data, **kwargs)

    getNodeData = dataAtNode

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def dataForHour(self, dataset_path, hour, **kwargs):
        """
        Extracts data for as single hour at all grid nodes.

        Arguments:
        ---------
            dataset_path: full dot.path to dataset (string)
            hour: datetime.datetime object or string of the
                  form YYYY-MM-DD:HH

            NOTE: a datetime object may include a tzinfo attribute
                  created using atmosci.tzutils.asHourInTimezone()
                  Incompatible tzinfo values from other sources will
                  be ignored.
            
            NOTE: When an hour string or datetime without a valid
                  tzinfo attribute is passed, "timezone" may be
                  passed in kwargs to define the timezone.
                  Otherwise the default timezone ('US/Eastern')
                  will be used. The default can be changed
                  using this class' setDefaultTimezone() method.

        Returns:
        -------
            NumPy array containing the retrieved data.
        """
        index = self.indexForTime(dataset_path, hour, **kwargs)
        data = self.getDataset(dataset_path).value[index,:,:]
        return self._processDataOut(dataset_path, data, **kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def dataForHourAtNode(self, dataset_path, hour, lon, lat, **kwargs):
        """
        Extracts data for a single hour at the single node that
        is closest to the coordinates requested.

        Arguments:
        ---------
            dataset_path: full dot.path to dataset (string)
            hour: datetime.datetime object or string of form 
                  YYYY-MM-DD:HH
            lon: longitude for grid node
            lat: latitude for grid node

            NOTE: a datetime object may include a tzinfo attribute
                  created using atmosci.tzutils.asHourInTimezone()
                  Incompatible tzinfo values from other sources
                  will be ignored.
            
            NOTE: When an hour string or datetime without a valid
                  tzinfo attribute is passed, "timezone" may be
                  passed in kwargs to define the timezone.
                  Otherwise the default timezone ('US/Eastern')
                  will be used. The default can be changed
                  using this class' setDefaultTimezone() method.

        Returns:
        -------
            A single data value for the hour at the node.
        """
        y, x = self.ll2index(lon, lat)
        index = self.indexForTime(dataset_path, hour, **kwargs)
        data = self.getDataset(dataset_path)[index, y, x]
        return self._processDataOut(dataset_path, data, **kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def dataSlice(self, dataset_path, start_time, end_time, min_lon, max_lon,
                        min_lat, max_lat, **kwargs):
        """
        Extracts a subset of data for a sequence hours at all
        nodes within the area described by the lon/lat
        bounding box.

        Arguments:
        ---------
            dataset_path: full dot.path to dataset (string)
            start_time: datetime.datetime object or string of
                        the form YYYY-MM-DD:HH
            end_time: datetime.datetime object, string of the
                      form YYYY-MM-DD:HH, an integer number
                      of hours or ':' indicating to end.
            min_lon: minimum longitude to include (float)
            max_lon: maximum longitude to include (float)
            min_lat: minimum latitude to include (float)
            max_lat: maximum latitude to include (float)

            NOTE: a datetime object may include a tzinfo attribute
                  created using atmosci.tzutils.asHourInTimezone()
                  Incompatible tzinfo values from other sources
                  will be ignored.
            
            NOTE: When an hour string or datetime without a valid
                  tzinfo attribute is passed, "timezone" may be
                  passed in kwargs to define the timezone.
                  Otherwise the default timezone ('US/Eastern')
                  will be used. The default can be changed
                  using this class' setDefaultTimezone() method.

        Returns:
        -------
            NumPy array containing the retrieved data.
        """
        min_y, min_x = self.ll2index(min_lon, min_lat)
        max_y, max_x = self.ll2index(max_lon, max_lat)
        start, end = self.indexesForTimes(dataset_path, start_time,
                                          end_time, **kwargs)
        dataset = self.getDataset(dataset_path)
        data = self._slice3DDataset(dataset, start, end, min_y, max_y,
                                    min_x, max_x)
        return self._processDataOut(dataset_path, data, **kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def indexForTime(self, dataset_path, hour, exact=True, **kwargs):
        """
        Calculate the index of a particular hour relative to the
        start time associated with the dataset.

        Arguments:
        ---------
            dataset_path: full dot.path to dataset (string)
            hour: datetime.datetime object, string of the form
                  YYYY-MM-DD:HH or an integer number of hours
                  after the first hour in the dataset.
            exact: boolean, Used only when hourly frequency > 1.
                   Default == True, an Exception is thrown when
                   the requested hour is not an exact match an hour
                   in the dataset. when set to False, data for the
                   hour closest to the requested hour is returned.

            NOTE: a datetime object may include a tzinfo attribute
                  created using atmosci.tzutils.asHourInTimezone()
                  Incompatible tzinfo values from other sources
                  will be ignored.
            
            NOTE: When an hour string or datetime without a valid
                  tzinfo attribute is passed, "timezone" may be
                  passed in kwargs to define the timezone.
                  Otherwise the default timezone ('US/Eastern')
                  will be used. The default can be changed using
                  the instance's setDefaultTimezone() method.

        Returns:
        -------
        The index corresponding to the hour (int).
        """
        # integers are treated as indexes and passed through
        if isinstance(hour, int): return hour

        # conbvert the input hour to an hour in the dataset's timzone
        timezone = kwargs.get('timezone',None)
        tzhour = self.asDatasetTime(dataset_path, hour, timezone) 
        start_time = \
            self.timeAttribute(dataset_path, 'start_time', self.start_time)
        end_time = self.timeAttribute(dataset_path, 'end_time', self.end_time)

        if tzhour >= start_time and tzhour <= end_time:
            frequency = self.datasetAttribute(dataset_path, 'frequency', 1)
            diff = tzutils.timeDifferenceInHours(tzhour, start_time)
            if frequency == 1: return diff
            else:
                index = diff / frequency
                mod = diff % frequency
                # always return an exact match
                if mod == 0: return index
                # raise exception when hour must exactly match a dataset hour
                if exact:
                    msg_args = (tzutils.hourAsString(hour), diff, frequency,
                                dataset_path)
                    errmsg = '%s (hour %d) is not a multiple of the hourly'
                    errmsg += ' frequency (%d) in the "%s" dataset.\nPass False'
                    errmsg += ' in "exact" arg to get data for closest hour.'
                    raise ValueError, errmsg % msg_args
                # "exact" is False - return index for closest hour
                else:
                    if mod <= frequency / 2.: return index
                    else: return index + 1
        else:
            msg_args = (str(hour), tzutils.hourAsString(hour), str(start_time),
                        str(end_time), dataset_path)
            errmsg = '%s (%s) is outside the valid range of hours\n'
            errmsg += '(%s to %s) for the "%s" dataset.'
            raise ValueError, errmsg % msg_args
    indexForHour = indexForTime

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def indexesForTimes(self, dataset_path, start_time, end_time, **kwargs):
        """
        Calculate the indexes of a 2 times relative to the start 
        time associated with the dataset.

        Arguments:
        ---------
            dataset_path: full dot.path to dataset (string)
            start_time: datetime.datetime object, string of the form
                        YYYY-MM-DD:HH or an integer number of hours
                        after the first hour in the dataset.
            end_time: datetime.datetime object, string of the form
                      YYYY-MM-DD:HH, an integer number of hours
                      after the first hour in the dataset or ':'
                      indicating the end of time dimension.

            NOTE: a datetime object may include a tzinfo attribute
                  created using atmosci.tzutils.asHourInTimezone()
                  Incompatible tzinfo values from other sources
                  will be ignored.
            
            NOTE: When an hour string or datetime without a valid
                  tzinfo attribute is passed, "timezone" may be
                  passed in kwargs to define the timezone.
                  Otherwise the default timezone ('US/Eastern')
                  will be used. The default can be changed
                  using this class' setDefaultTimezone() method.

        Returns:
        -------
        A 2 tuple containing the index corresponding to start_time
        followed by the index correspondinf to end_time. Indexes
        are integers.
        """
        if isinstance(start_time, (datetime.datetime, basestring)):
            start = self.indexForTime(dataset_path, start_time, **kwargs)
        elif isinstance(start_time, int):
            start = start_time
        else:
            errmsg = ARG_TYPE_ERROR % (str(type(start_time)), 'start_time')
            raise TypeError, errmsg

        if end_time == ':':
            end = self.datasetShape(dataset_path)[0] + 1
        elif isinstance(end_time, (datetime.datetime, basestring)):
            end = self.indexForTime(dataset_path, end_time, **kwargs) + 1
        elif isinstance(end_time, int):
            end = start + end_time
        else:
            errmsg = ARG_TYPE_ERROR % (str(type(end_time)), 'end_time')
            raise TypeError, errmsg

        return start, end
    indexesForHours = indexesForTimes

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def setDefaultTimezone(self, timezone):
        """
        Set the default timezone for time arguments passed to data
        input and output functions. This value is NOT persistent.
        It available only in the instance of file Reader/Manager
        that was instatiated when it was set.

        Arugment :
        ________
            timezone: the local timezone to be used in all
                      data access and update functions when
                      time paramaeters as strings or do not
                      contain valid tzinfo attributes.

                The value of timezone may be a string containing
                a valid timezone name or a tzinfo object created
                using atmosci.tzutils.asTimezoneObj().
        """
        if tzutils.isValidTimezone(timezone): self.default_timezone = timezone
        else: self.default_timezone = tzutils.asTimezoneObj(timezone)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def sliceAtNode(self, dataset_path, start_time, end_time, lon, lat,
                          **kwargs):
        """
        Extracts a subset of data for a sequence hours at the
        grid node that is closest to the coordinates requested.

        Arguments:
        ---------
            dataset_path: full dot.path to dataset (string)
            start_time: datetime.datetime object or string of
                        the form YYYY-MM-DD:HH
            end_time: datetime.datetime object, string of the
                      form YYYY-MM-DD:HH, an integer number
                      of hours or ':' indicating to end.
            lon: longitude for grid node
            lat: latitude for grid node

            NOTE: a datetime object may include a tzinfo attribute
                  created using atmosci.tzutils.asHourInTimezone()
                  Incompatible tzinfo values from other sources
                  will be ignored.
            
            NOTE: When an hour string or datetime without a valid
                  tzinfo attribute is passed, "timezone" may be
                  passed in kwargs to define the timezone.
                  Otherwise the default timezone ('US/Eastern')
                  will be used. The default can be changed
                  using this class' setDefaultTimezone() method.

        Returns:
        -------
            NumPy array containing the retrieved data.
        """
        start, end = \
            self.indexesForTimes(dataset_path, start_time, end_time, **kwargs)
        y, x = self.ll2index(lon, lat)
        data = self.getDataset(dataset_path)[start:end, y, x]
        return self._processDataOut(dataset_path, data, **kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def timeAttribute(self, object_path, attr_name, default=None):
        """
        Extract a time attribute from an object in a file.

        Arguments:
        ---------
            object_path: full dot.path to object (string)
            attr_name: time attribute to retieve (string)
            default: value to be returned when attribute is
                     not found. Defaults to None.

        Returns:
        -------
            An instance of datetime.datetime with a valid 
            tzinfo attribute (represents the timezone used
            for time management of the object's data).
            OR
            default if the attribute does not exist
        """
        #TODO: let default be an instance of Exception or a callable
        #      (i.e. a function or object with __call__ method)
        #
        # check to see if it has been accessed before
        time_attr = self._fromTimeAttrCache(object_path, attr_name)
        if time_attr is not None: return time_attr
        # first time access, get the value, cache it, then return it
        time_attr = self.objectAttribute(object_path, attr_name, default)
        if time_attr == default: return default
        # convert time string to hour in current object timezone
        tz = self.objectAttribute(object_path, 'timezone', self.timezone)
        time_attr = tzutils.asHourInTimezone(time_attr, tz)
        self._cacheTimeAttr(object_path, attr_name, time_attr)
        return time_attr

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def timeAttributes(self, object_path=None, as_time_obj=True):
        """
        Extract all time attributes from an object in a file.

        Arguments:
        ---------
            object_path: full dot.path to object (string)
            as_time_obj: True: convert time strings to timezone
                               aware datetime.datetime objects
                         False: keep strings as they were stored.
                         Default is True.

        Returns:
        -------
            A dictionary with time attribute names as keys and
            the corresponding times as values. Vaule type is
            determined by value of as_time_obj argument.
            The dictinary also includes the 'timezone'
        """
        time_attrs = { }
        if object_path is None:
            tz = self.fileAttribute('timezone', self.timezone)
            attrs = self.fileAttributes()
        else:
            tz = self.objectAttribute(object_path, 'timezone', self.timezone)
            attrs = self.objectAttributes(object_path)
        if as_time_obj:
            tz = tzutils.asTimezoneObj(tz)
            for key, value in attrs.items():
                if key.endswith('time'):
                    time_attrs[key] = tzutils.asHourInTimezone(value, tz)
            if time_attrs: # add timezone aware attributes to cache
                self.time_attr_cache[object_path] = time_attrs
        else:
            for key, value in attrs.items():
                if key.endswith('time'): time_attrs[key] = value
        # add timzone only if this object has time attributes
        if time_attrs:
            time_attrs['timezone'] = tz
            time_attrs['tzinfo'] = tzutils.asTimezoneObj(tz)

        return time_attrs

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def timeSlice(self, dataset_path, start_time, end_time, **kwargs):
        """
        Extracts a subset of data for a sequence hours at all
        grid nodes in the dataset.

        Arguments:
        ---------
            dataset_path: full dot.path to dataset (string)
            start_time: datetime.datetime object or string of
                        the form YYYY-MM-DD:HH
            end_time: datetime.datetime object, string of the
                      form YYYY-MM-DD:HH, an integer number
                      of hours or ':' indicating to end.
            min_lon: minimum longitude to include (float)
            max_lon: maximum longitude to include (float)
            min_lat: minimum latitude to include (float)
            max_lat: maximum latitude to include (float)

            NOTE: a datetime object may include a tzinfo attribute
                  created using atmosci.tzutils.asHourInTimezone()
                  Incompatible tzinfo values from other sources
                  will be ignored.
            
            NOTE: When an hour string or datetime without a valid
                  tzinfo attribute is passed, "timezone" may be
                  passed in kwargs to define the timezone.
                  Otherwise the default timezone ('US/Eastern')
                  will be used. The default can be changed
                  using this class' setDefaultTimezone() method.

        Returns:
        -------
            NumPy array containing the retrieved data.
        """
        start, end = \
            self.indexesForTimes(dataset_path, start_time, end_time, **kwargs)
        data = self.getDataset(dataset_path)[start:end, :, :]
        return self._processDataOut(dataset_path, data, **kwargs)

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _areaSlice(self, dataset, min_y, min_x, max_y, max_x, **kwargs):
        shape = dataset.shape
        ndims = len(shape)
        errmsg = 'Cannot subset %dD dataset using lon,lat bounds.'
        assert(ndims in (2,3)), errmsg % ndims

        if ndims == 3:
            if max_y == min_y:
                if max_x == min_x: # retrieve data for one node
                    return dataset[:, min_y, min_x]
                elif max_x < shape[2]:
                    return dataset[:, min_y, min_x:max_x]
                else: # max_x >= dataset.shape[2]
                    return dataset[:, min_y, min_x:]
            elif max_y < shape[1]:
                if max_x == min_x:
                    return dataset[:, min_y:max_y, min_x]
                elif max_x < shape[2]:
                    return dataset[:, min_y:max_y, min_x:max_x]
                else: # max_x >= dataset.shape[2]
                    return dataset[:, min_y:max_y, min_x:]
            else: # max_y >= dataset.shape[1]
                if max_x == min_x:
                    return dataset[:, min_y:, min_x]
                elif max_x < shape[2]:
                    return dataset[:, min_y:, min_x:max_x]
                else: # max_x >= dataset.shape[2]
                    return dataset[:, min_y:, min_x:]

        elif ndims == 2:
            if max_y < shape[0]:
                if max_x < shape[1]:
                    return dataset[min_y:max_y, min_x:max_x]
                else:
                    return dataset[min_y:max_y, min_x:]
            else:
                if max_x < shape[1]:
                    return dataset[min_y:, min_x:max_x]
                else:
                    return dataset[min_y:, min_x:]
        
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _cacheTimeAttr(self, object_path, attr_name, time_obj):
        cache = self.time_attr_cache.get(object_path, { })
        cache[attr_name] = time_obj
        self.time_attr_cache[object_path] = cache

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _coordBoundsSubset(self, dataset):
        """ Returns a subset of a grid that is within the grid manager's
        lon/lat bounding box. Grid shape must be [y, x].
        """
        if self._index_bounds is not None:
            min_y, min_x, max_y, max_x = self._index_bounds
            return self._areaSlice(dataset, min_y, min_x, max_y, max_x)

        # asking for data at a single point
        elif self._x is not None:
            return self._dataAtNode(dataset, self._y, self._x)

        # no bounds set, return all values in dataset
        return dataset.value

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _dataAtNode(self, dataset, y, x):
        shape = dataset.shape
        if len(shape) == 3:
            return dataset[:, y, x]
        elif len(shape) == 2:
            return dataset[y, x]

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _fromTimeAttrCache(self, object_path, attr_name):
        cache = self.time_attr_cache.get(object_path, { })
        return cache.get(attr_name, None)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _slice3DDataset(self, dataset, start, end, min_y, max_y, min_x, max_x):
        if end == start: # single date
            if max_y == min_y:
                if max_x == min_x: # retrieve data for one node
                    return dataset[start, min_y, min_x]
                elif max_x < dataset.shape[2]:
                    return dataset[start, min_y, min_x:max_x]
                else: # max_x >= dataset.shape[2]
                    return dataset[start, min_y, min_x:]
            elif max_y < dataset.shape[1]:
                if max_x == min_x:
                    return dataset[start, min_y:max_y, min_x]
                elif max_x < dataset.shape[2]:
                    return dataset[start, min_y:max_y, min_x:max_x]
                else: # max_x >= dataset.shape[2]
                    return dataset[start, min_y:max_y, min_x:]
            else: # max_y >= dataset.shape[1]
                if max_x == min_x:
                    return dataset[start, min_y:, min_x]
                elif max_x < dataset.shape[2]:
                    return dataset[start, min_y:, min_x:max_x]
                else: # max_x >= dataset.shape[2]
                    return dataset[start, min_y:, min_x:]
        elif end < dataset.shape[0]:
            if max_y == min_y:
                if max_x == min_x: # retrieve data for one node
                    return dataset[start:end, min_y, min_x]
                elif max_x < dataset.shape[2]:
                    return dataset[start:end, min_y, min_x:max_x]
                else: # max_x >= dataset.shape[2]
                    return dataset[start:end, min_y, min_x:]
            elif max_y < dataset.shape[1]:
                if max_x == min_x:
                    return dataset[start:end, min_y:max_y, min_x]
                elif max_x < dataset.shape[2]:
                    return dataset[start:end, min_y:max_y, min_x:max_x]
                else: # max_x >= dataset.shape[2]
                    return dataset[start:end, min_y:max_y, min_x:]
            else: # max_y >= dataset.shape[1]
                if max_x == min_x:
                    return dataset[start:end, min_y:, min_x]
                elif max_x < dataset.shape[2]:
                    return dataset[start:end, min_y:, min_x:max_x]
                else: # max_x >= dataset.shape[2]
                    return dataset[start:end, min_y:, min_x:]
        else: # end > dataset.shape[0] ... retrieve all dates to end of dataset
            if max_y == min_y:
                if max_x == min_x: # retrieve data for one node
                    return dataset[start:, min_y, min_x]
                elif max_x < dataset.shape[2]:
                    return dataset[start:, min_y, min_x:max_x]
                else: # max_x >= dataset.shape[2]
                    return dataset[start:, min_y, min_x:]
            elif max_y < dataset.shape[1]:
                if max_x == min_x:
                    return dataset[start:, min_y:max_y, min_x]
                elif max_x < dataset.shape[2]:
                    return dataset[start:, min_y:max_y, min_x:max_x]
                else: # max_x >= dataset.shape[2]
                    return dataset[start:, min_y:max_y, min_x:]
            else: # max_y >= dataset.shape[1]
                if max_x == min_x:
                    return dataset[start:, min_y:, min_x]
                elif max_x < dataset.shape[2]:
                    return dataset[start:, min_y:, min_x:max_x]
                else: # max_x >= dataset.shape[2]
                    return dataset[start:, min_y:, min_x:]

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _loadHourGridAttributes_(self):
        timezone = self.fileAttribute('timezone', None)
        if timezone is not None:
            self.timezone = timezone
            self.tzinfo = tzinfo = tzutils.asTimezoneObj(timezone)
        else:
            errmsg = 'Hour-based time files must have a "timezone" attribute. '
            errmsg += 'Indexing will not function properly for cross-timezone '
            errmsg += 'retrievals.'
            raise KeyError, errmsg

        self.default_timezone = DEFAULT_TZINFO
        start_time = self.fileAttribute('start_time', None)
        if start_time is not None: 
            self.start_time = tzutils.asHourInTimezone(start_time, tzinfo)
        end_time = self.fileAttribute('end_time', None)
        if end_time is not None:
            self.end_time = tzutils.asHourInTimezone(end_time, tzinfo)

        timezone_map = { }
        for path in self._dataset_names:
            timezone = self.datasetAttribute(path, 'timezone', None)
            if timezone is None: timezone_map[path] = DEFAULT_TZINFO
            else: timezone_map[path] = tzutils.asTimezoneObj(timezone)
        self.timezone_cache = timezone_map

        self.time_attr_cache = { }


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class Hdf5HourlyGridFileReader(Hdf5HourlyGridReaderMethods,
                               Hdf5GridFileReader):
    """ Provides read-only access to 3D gridded data in HDF5 files where
    the first dimension is time, the 2nd dimension is rows and the 3rd
    dimension is columns. Grids can contain any type of data. Indexing
    based on Time/Latitude/Longitude is available. Time indexes may be
    derived from date/time with earliest date at index 0. Row indexes
    may be derived from Latitude coordinates with minimum Latitude at
    row index 0. Columns indexes may be derived from Longitude
    coordinates with minimum Longitude at column index 0.

    Inherits all of the capabilities of Hdf5GridFileReader
    """

    def __init__(self, hdf5_filepath):
        Hdf5GridFileReader.__init__(self, hdf5_filepath)

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _loadManagerAttributes_(self):
        Hdf5GridFileReader._loadManagerAttributes_(self)
        self._loadHourGridAttributes_()


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class Hdf5HourlyGridManagerMethods(Hdf5HourlyGridReaderMethods):
    """ Provides read/write access to 3D gridded data in HDF5 files where
    the first dimension is time, the 2nd dimension is rows and the 3rd
    dimension is columns. Grids can contain any type of data. Indexing
    based on Time/Latitude/Longitude is available. Time indexes may be
    derived from date/time with earliest date at index 0. Row indexes
    may be derived from Latitude coordinates with minimum Latitude at
    row index 0. Columns indexes may be derived from Longitude
    coordinates with minimum Longitude at column index 0.

    Inherits all of the capabilities of Hdf5GridFileManager
    """

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def insertAtNode(self, dataset_path, data, start_time, lon, lat, **kwargs):
        """
        Inserts data for a sequence hours at the grid node
        that is closest to the input coordinates.

        Arguments:
        ---------
            dataset_path: full dot.path to dataset (string)
            data: 1D NumPy data array 
            start_time: datetime.datetime object or string of
                        the form YYYY-MM-DD:HH
            lon: longitude for grid node
            lat: latitude for grid node

            NOTE: a datetime object may include a tzinfo attribute
                  created using atmosci.tzutils.asHourInTimezone()
                  Incompatible tzinfo values from other sources
                  will be ignored.
            
            NOTE: When an hour string or datetime without a valid
                  tzinfo attribute is passed, "timezone" may be
                  passed in kwargs to define the timezone.
                  Otherwise the default timezone ('US/Eastern')
                  will be used. The default can be changed
                  using this class' setDefaultTimezone() method.
        """
        index = self.indexForTime(dataset_path, start_time, **kwargs)
        y, x = self.ll2index(lon, lat)
        processed = self._processDataIn(dataset_path, data, **kwargs)
        self._insertAtNode(dataset_path, processed, index, x, y, **kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def insertTimeSlice(self, dataset_path, data, start_time, **kwargs):
        """
        Inserts data for a sequence hours at at all grid nodes.

        Arguments:
        ---------
            dataset_path: full dot.path to dataset (string)
            data: 3D NumPy data array 
            start_time: datetime.datetime object or string of
                        the form YYYY-MM-DD:HH

            NOTE: a datetime object may include a tzinfo attribute
                  created using atmosci.tzutils.asHourInTimezone()
                  Incompatible tzinfo values from other sources
                  will be ignored.
            
            NOTE: When an hour string or datetime without a valid
                  tzinfo attribute is passed, "timezone" may be
                  passed in kwargs to define the timezone.
                  Otherwise the default timezone ('US/Eastern')
                  will be used. The default can be changed
                  using this class' setDefaultTimezone() method.

        """
        time_index = self.indexForTime(dataset_path, start_time, **kwargs)
        processed = self._processDataIn(dataset_path, data, **kwargs)
        self._insertTimeSlice(dataset_path, processed, time_index, **kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def insert3DSlice(self, dataset_path, data, start_time, min_lon, max_lon,
                            min_lat, max_lat, **kwargs):
        """
        Inserts a data for a sequence hours at a subset of grid
        nodes defined by the lon/lat bounding box.

        Arguments:
        ---------
            dataset_path: full dot.path to dataset (string)
            data: 3D NumPy data array 
            start_time: datetime.datetime object or string of
                        the form YYYY-MM-DD:HH
            min_lon: minimum longitude in data (float)
            max_lon: maximum longitude in data (float)
            min_lat: minimum latitude in data include (float)
            max_lat: maximum latitude in data include (float)

            NOTE: a datetime object may include a tzinfo attribute
                  created using atmosci.tzutils.asHourInTimezone()
                  Incompatible tzinfo values from other sources
                  will be ignored.
            
            NOTE: When an hour string or datetime without a valid
                  tzinfo attribute is passed, "timezone" may be
                  passed in kwargs to define the timezone.
                  Otherwise the default timezone ('US/Eastern')
                  will be used. The default can be changed
                  using this class' setDefaultTimezone() method.
        """
        min_y, min_x = self.ll2index(min_lon, min_lat)
        max_y, max_x = self.ll2index(max_lon, max_lat)
        time_index = self.indexForTime(dataset_path, start_time, **kwargs)
        processed = self._processDataIn(dataset_path, data, **kwargs)
        self._insert3DSlice(dataset_path, processed, time_index,
                            min_y, max_y, min_x, max_x, **kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def setTimeAttribute(self, object_path, attribute_name, hour):
        if isinstance(hour, datetime.datetime):
            time_str = tzutils.hourAsString(hour)
        elif isinstance(hour, basestring):
            errmsg = '"%s" not a valid time string.'
            errmsg += ' Time strings must be use YYYY-MM-DD:HH format.'
            time_parts = hour.split('-')
            if len(time_parts) == 3:
                if len(time_parts[2].split(':')) == 2:
                    time_str = hour
                else:
                    raise ValueError, errmsg % hour
            elif len(time_parts) == 4:
                time_str = '-'.join(time_parts[:3])
                time_str = '%s:%s' % (time_str, time_parts[3])
            else:
                raise ValueError, errmsg % hour
        else:
            raise TypeError, ARG_TYPE_ERROR % (str(type(hour)), 'hour')

        self.setObjectAttribute(object_path, attribute_name, time_str)

        # cache the new value as a timezone aware datetime
        tz = self.objectAttribute(object_path, 'timezone', self.timezone)
        time_obj = tzutils.asHourInTimezone(hour, tz)
        self._cacheTimeAttr(object_path, attr_name, time_obj)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def setTimeAttributes(self, object_path, **kwargs):
        for attr_name, hour in kwargs.items():
            self.setTimeAttribute(object_path, attr_name, hour)

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _insert2DSlice(dataset, data, min_y, max_y, min_x, max_x):
        shape = dataset.shape
        ndims = len(shape)
        errmsg = 'Cannot insert into %dD dataset using lon,lat bounds.'
        assert(ndims in (2,3)), errmsg % ndims

        if ndims == 3:
            if max_y == min_y:
                if max_x == min_x: # retrieve data for one node
                    dataset[:, min_y, min_x] = data
                elif max_x < shape[1]:
                    dataset[:, min_y, min_x:max_x] = data
                else: # max_x >= dataset.shape[1]
                    dataset[:, min_y, min_x:] = data
            elif max_y < shape[0]:
                if max_x == min_x:
                    dataset[:, min_y:max_y, min_x] = data
                elif max_x < shape[1]:
                    dataset[:, min_y:max_y, min_x:max_x] = data
                else: # max_x >= dataset.shape[1]
                    dataset[:, min_y:max_y, min_x:] = data
            else: # max_y >= dataset.shape[0]
                if max_x == min_x:
                    dataset[:, min_y:, min_x] = data
                elif max_x < shape[1]:
                    dataset[:, min_y:, min_x:max_x] = data
                else: # max_x >= dataset.shape[1]
                    dataset[:, min_y:, min_x:] = data
        elif ndims == 2:
            if max_y == min_y:
                if max_x == min_x: # retrieve data for one node
                    dataset[min_y, min_x] = data
                elif max_x < shape[1]:
                    dataset[min_y, min_x:max_x] = data
                else: # max_x >= dataset.shape[1]
                    dataset[min_y, min_x:] = data
            elif max_y < shape[0]:
                if max_x == min_x:
                    dataset[min_y:max_y, min_x] = data
                elif max_x < shape[1]:
                    dataset[min_y:max_y, min_x:max_x] = data
                else: # max_x >= dataset.shape[1]
                    dataset[min_y:max_y, min_x:] = data
            else: # max_y >= dataset.shape[0]
                if max_x == min_x:
                    dataset[min_y:, min_x] = data
                elif max_x < shape[1]:
                    dataset[min_y:, min_x:max_x] = data
                else: # max_x >= dataset.shape[1]
                    dataset[min_y:, min_x:] = data

        return dataset

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _insert3DSlice(self, data, start_index, min_y, max_y, min_x, max_x):
        if data.dims == 2: end_index = start_index
        elif data.dims == 3: end_index = start_index + data.shape[0]
        else:
            errmsg = 'Cannot insert %dD data into a 3D dataset.'
            raise ValueError, errmsg % data.dims

        dataset = self.getDataset(dataset_path)
        if end_index == start_index: # single date
            return self._insertHourInBounds(dataset_path, data, start_index,
                                            min_y, max_y, min_x, max_x)

        elif end_index < dataset.shape[0]:
            return self._insertHoursInBounds(dataset_path, data,
                                             start_index, end_index,
                                             min_y, max_y, min_x, max_x)

        else: # end_index > dataset.shape[0]
            # insert to end of dataset
            return self._insertToEndInBounds(dataset_path, data, start_index,
                                             min_y, max_y, min_x, max_x)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _insertAtNode(self, dataset_path, data, time_index, x, y, **kwargs):
        end_index = time_index + data.shape[0]
        dataset = self.getDataset(dataset_path)
        dataset[time_index:end_index, y, x] = data
        return dataset

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _insertDataInBounds(dataset_name, dataset, data):
        # data within a bounding box
        if self._index_bounds is not None:
            min_y, min_x, max_y, max_x = self._index_bounds
            self._insert2DSlice(dataset, min_y, min_x, max_y, max_x)

        # data at a single point
        elif self._x is not None:
            self._insertAtNode(dataset, self._y, self._x)

        else:
            errmsg = 'Coordinate bounding box has not been defined.'
            raise AttributeError, errmsg

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _insertHourInBounds(self, dataset_path, data, hour_index,
                                  min_y, max_y, min_x, max_x):
        dataset = self.getDataset(dataset_path)
        shape = dataset.shape
        if max_y == min_y:
            if max_x == min_x: # retrieve data for one node
                dataset[hour_index, min_y, min_x] = data
            elif max_x < shape[2]:
                dataset[hour_index, min_y, min_x:max_x] = data
            else: # max_x >= dataset.shape[2]
                dataset[hour_index, min_y, min_x:] = data
        elif max_y < shape[1]:
            if max_x == min_x:
                dataset[hour_index, min_y:max_y, min_x] = data
            elif max_x < shape[2]:
                dataset[hour_index, min_y:max_y, min_x:max_x] = data
            else: # max_x >= dataset.shape[2]
                dataset[hour_index, min_y:max_y, min_x:] = data
        else: # max_y >= dataset.shape[1]
            if max_x == min_x:
                dataset[hour_index, min_y:, min_x] = data
            elif max_x < shape[2]:
                dataset[hour_index, min_y:, min_x:max_x] = data
            else: # max_x >= dataset.shape[2]
                dataset[hour_index, min_y:, min_x:] = data

        return dataset

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _insertHoursInBounds(self, dataset, data, start_index, end_index,
                                   min_y, max_y, min_x, max_x):
        dataset = self.getDataset(dataset_path)
        if max_y == min_y:
            if max_x == min_x: # retrieve data for one node
                dataset[start_index:end_index, min_y, min_x] = data
            elif max_x < dataset.shape[2]:
                dataset[start_index:end_index, min_y, min_x:max_x] = data
            else: # max_x >= dataset.shape[2]
                dataset[start_index:end_index, min_y, min_x:] = data
        elif max_y < dataset.shape[1]:
            if max_x == min_x:
                dataset[start_index:end_index, min_y:max_y, min_x] = data
            elif max_x < dataset.shape[2]:
                dataset[start_index, min_y:max_y, min_x:max_x] = data
            else: # max_x >= dataset.shape[2]
                dataset[start_index:end_index, min_y:max_y, min_x:] = data
        else: # max_y >= dataset.shape[1]
            if max_x == min_x:
                dataset[start_index:end_index, min_y:, min_x] = data
            elif max_x < dataset.shape[2]:
                dataset[start_index:end_index, min_y:, min_x:max_x] = data
            else: # max_x >= dataset.shape[2]
                dataset[start_index:end_index, min_y:, min_x:] = data

        return dataset

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _insertTimeSlice(self, dataset_path, data, time_index, **kwargs):
        if not isinstance(data, N.ndarray):
            errmsg = 'Data for "%s" dataset must be a NumPy array'
            raise TypeError, errmsg % dataset_path
        errmsg = 'Cannot insert data with %dD data into %dD dataset.'

        dataset = self.getDatasetShape(dataset_path)
        dataset_dims = len(dataset.shape)
        if dataset_dims == 3:
            if data.ndim == 3:
                end_index = time_index + data.shape[0]
                dataset[time_index:end_index] = data
            elif data.ndim == 2:
                dataset[time_index,:,:] = data
            else:
                raise ValueError, errmsg % (data.ndim, dataset_dims)
        elif dataset_dims == 1:
            if isinstance(data_in, N.ndarray):
                if data.ndim == 1:
                    if len(data) > 1:
                        end_index = time_index + len(data)
                        dataset[time_index:end_index] = data
                    else:
                        dataset[time_index] = data
                else:
                    raise ValueError, errmsg % (data.ndim, dataset_dims)
            else: # insert scalar value
                dataset[time_index] = data
        else:
            raise ValueError, errmsg % (data.ndim, dataset_dims)

        return dataset

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _insertToEndInBounds(self, dataset, data, start_index,
                                   min_y, max_y, min_x, max_x):
        if max_y == min_y:
            if max_x == min_x: # retrieve data for one node
                dataset[start_index:, min_y, min_x] = data
            elif max_x < dataset.shape[2]:
                dataset[start_index:, min_y, min_x:max_x] = data
            else: # max_x >= dataset.shape[2]
                dataset[start_index:, min_y, min_x:] = data
        elif max_y < dataset.shape[1]:
            if max_x == min_x:
                dataset[start_index:, min_y:max_y, min_x] = data
            elif max_x < dataset.shape[2]:
                dataset[start_index:, min_y:max_y, min_x:max_x] = data
            else: # max_x >= dataset.shape[2]
                dataset[start_index:, min_y:max_y, min_x:] = data
        else: # max_y >= dataset.shape[1]
            if max_x == min_x:
                dataset[start_index:, min_y:, min_x] = data
            elif max_x < dataset.shape[2]:
                dataset[start_index:, min_y:, min_x:max_x] = data
            else: # max_x >= dataset.shape[2]
                dataset[start_index:, min_y:, min_x:] = data

        return dataset

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


class Hdf5HourlyGridFileManager(Hdf5HourlyGridManagerMethods,
                                Hdf5GridFileManager):
    """ Provides read/write access to 3D gridded data in HDF5 files where
    the first dimension is time, the 2nd dimension is rows and the 3rd
    dimension is columns. Grids can contain any type of data. Indexing
    based on Time/Latitude/Longitude is available. Time indexes may be
    derived from date/time with earliest date at index 0. Row indexes
    may be derived from Latitude coordinates with minimum Latitude at
    row index 0. Columns indexes may be derived from Longitude
    coordinates with minimum Longitude at column index 0.

    Inherits all of the capabilities of Hdf5GridFileManager
    """

    def __init__(self, hdf5_filepath, mode='r'):
        Hdf5GridFileManager.__init__(self, hdf5_filepath, mode)

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _loadManagerAttributes_(self):
        Hdf5GridFileManager._loadManagerAttributes_(self)
        self._loadHourGridAttributes_()

