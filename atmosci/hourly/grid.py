
import inspect
import datetime
ONE_HOUR = datetime.timedelta(hours=1)

import numpy as N
try:
    nanMedian = N.nanmedian
except:
    from scipy import stats as scipy_stats
    nanMedian = scipy_stats.nanmedian

from atmosci.utils import tzutils

from atmosci.hdf5.hourgrid import Hdf5HourlyGridFileReader, \
                                  Hdf5HourlyGridFileManager

from atmosci.seasonal.methods.grid import GridFileReaderMethods, \
                                          GridFileManagerMethods


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Provenance generators
#
# record generator for time with value accumulation
def timeAccumStatsProvenanceGenerator(source, hour, timestamp, hourly,
                                      accumulated):
    return (tzutils.hourAsString(hour),
            N.nanmin(hourly), N.nanmax(hourly),
            N.nanmean(hourly), nanMedian(hourly,axis=None),
            N.nanmin(accumulated), N.nanmax(accumulated),
            N.nanmean(accumulated), nanMedian(accumulated,axis=None),
            timestamp, source)

def timeStampProvenanceGenerator(source, hour, timestamp, data):
    return (tzutils.hourAsString(hour), timestamp, source)

def timeStatsProvenanceGenerator(source, hour, timestamp, data):
    return (tzutils.hourAsString(hour),
            N.nanmin(data), N.nanmax(data),
            N.nanmean(data), nanMedian(data,axis=None),
            timestamp, source)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class HourlyGridFileReaderMethods(GridFileReaderMethods):

    def _loadProjectFileAttributes_(self):
        #attrs = self.getFileAttributes()
        attrs = self.fileAttributes()
        if not hasattr(self, 'start_time'):
            self.start_time = attrs.get('start_time', None)
        if not hasattr(self, 'end_time'):
            self.end_time = attrs.get('end_time', None)

        for attr_name, attr_value in attrs.items():
            if attr_name not in ('start_time', 'end_time'):
                self.__dict__[attr_name] = attr_value

        self._configDatasets_()
        self._loadDatasetAttrs_()
        self._postLoadFileAttrs_()

    def _preInitHourlyFileReader_(self, **kwargs):
        self._unpackers = { }


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class HourlyGridFileReader(HourlyGridFileReaderMethods,
                           Hdf5HourlyGridFileReader):

    def __init__(self, hdf5_filepath):
        self._preInitHourlyFileReader_()
        Hdf5HourlyGridFileReader.__init__(self, hdf5_filepath)

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _loadManagerAttributes_(self):
        Hdf5HourlyGridFileReader._loadManagerAttributes_(self)
        self._loadHourGridAttributes_()


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class HourlyGridFileManagerMethods(HourlyGridFileReaderMethods,
                                   GridFileManagerMethods):

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def adjustForecast(self, dataset_path, last_obs_time, **kwargs):
        """ Adjust forecast hours in reponse to update of observations

        Arguments :
        -----------
        dataset_path  : full dot path for dataset to update
        last_obs_time : datetime.hour representing hour of last observation

        Returns one of the following :
        ----------------------------
            None : forecast state was not changed
            last_obs_time : forecast was completely overwritten by observations
            new fcast_start_time : forecast was partially overwritten
                                   by observations
        """
        fcast_end_time = \
            self.timeAttribute(dataset_path, 'fcast_end_time', None)

        if fcast_end_time is not None:
            if fcast_end_time <= last_obs_time:
                # forecast was completely overwritten by observation data
                self.deleteDatasetAttribute(dataset_path, 'fcast_start_time')
                self.deleteDatasetAttribute(dataset_path, 'fcast_end_time')

            else: # forecast was partially overwritten by observation data
                fcast_start_time = \
                    self.timeAttribute(dataset_path, 'fcast_start_time')
                if fcast_start_time <= obs_time:
                    fcast_start_time = obs_time + ONE_HOUR
                    self.setTimeAttribute(dataset_path, 'fcast_start_time',
                                          fcast_start_time)
        
        #else: forecast has never been set

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def endTimeFromView(self, dataset_path, start_time, data):
        data_path = dataset_path
        if 'provenance' in dataset_path:
            reference = self.datasetAttribute(dataset_path, 'reference', None)
            if reference is not None: data_path = reference

        # 3D rray has multiple hours
        if data.ndim == 3:
            view = self.datasetAttribute(data_path, 'view')
            # for now, the code assumes a tyx or txy data view
            if view in ('txy','tyx'): 
                num_hours = data.shape[0]
            elif view in ('xyt','yxt'):
                num_hours = data.shape[2]
            
            return start_time + ONE_HOUR

        elif data.ndim == 2: # 2D array has a single hour
            return start_time

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def generateGroupProvenanceRecords(self, prov_path, start_time,
                                             data1, data2, **kwargs):
        """ Generates provenance records for groups based on statistics
        from 2 data arrays.

        Arguments
        --------------------------------------------------------------------
        prov_path  : string - path to a provenance dataset.
        start_time : datetime, scalar - hour/doy of provenance entry. If
                     input data is 3D, it is the first hour/doy and entries
                     will be generated for each day.
        data1, data2 : 2D or 3D numpy arrays - dimensions must match
                       data to be used to calculate provenance statistics.
                       If 3D, time must be the 1st dimension.
        """
        generator = self.provenanceGenerator(prov_path)
        timestamp = kwargs.get('timestamp', self.timestamp)

        timezone = \
            self.datasetAttribute(prov_path, 'timezone', self.default_timezone)
        start_hour = tzutils.asHourInTimezone(start_time, timezone)

        records = [ ]
        if data1.ndim == 2:
            records.append(generator(start_hour,timestamp,data1,data_2))
            end_hour = start_hour
        else:
            for hour in range(data_1.shape[0]):
                time_ = start_hour + datetime.timedelta(hours=hour)
                record = generator(time_, timestamp, data1[hour], data2[hour])
                records.append(record)
            end_hour = time_

        return start_hour, end_hour, records

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def generateProvenanceRecords(self, prov_path, start_time, data, **kwargs):
        """ Generates provenance records based on statistics from a single
        data array.

        Arguments
        --------------------------------------------------------------------
        prov_path  : string - path to a provenance dataset.
        start_time : datetime, scalar - hour/doy of provenance entry. If
                     input data is 3D, it is the first hour/doy and entries
                     will be generated for each day.
        data       : 2D or 3D numpy array - data to be used to calculate
                     provenance statistics. If 3D, time must be the 1st
                     dimension.
        """
        timezone = \
            self.datasetAttribute(prov_path, 'timezone', self.default_timezone)
        start_hour = tzutils.asHourInTimezone(start_time, timezone)
    
        timestamp = kwargs.get('timestamp', self.timestamp)
        
        generator = self.provenanceGenerator(prov_path)
        generator_args = inspect.getargspec(generator).args
        records = [ ]

        if 'source' in generator_args:
            source = kwargs.get('source',
                                self.fileAttribute('source','unknown'))
            if data.ndim == 2:
                records.append(generator(source, start_hour, timestamp, data))
                end_hour = start_hour
            else:
                num_hours = data.shape[0]
                for hour in range(num_hours):
                    time_ = start_hour + datetime.timedelta(hours=hour)
                    record = generator(source, time_, timestamp, data[hour])
                    records.append(record)
                end_hour = time_
        else:
            if data.ndim == 2:
                 records.append(generator(start_hour, timestamp, data))
                 end_hour = start_hour
            else:
                num_hours = data.shape[0]
                for hour in range(num_hours):
                    time_ = start_hour + datetime.timedelta(hours=hour)
                    records.append(generator(time_, timestamp, data[hour]))
                end_hour = time_

        return start_hour, end_hour, records

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def insertProvenance(self, prov_path, start_time, data, **kwargs):
        """ Inserts records into a provenance dataset using statistics
        from the input data array. It's purpose if to overwrite previously
        stored provenance with statistics from more recent information.

        NOTE: this function does not update the hour attributes >>>
              last_valid_time, last_obs_time, or fcast_end_time,
              Use the "updateProvenance" method when an updates of
              validity hour tracking attributes is also required.

        Arguments
        --------------------------------------------------------------------
        prov_path  : string - path to a provenance dataset.
        start_time : datetime, scalar - hour/doy of provenance entry. If
                     input data is 3D, it is the first hour/doy and entries
                     will be generated for each day.
        data       : 2D or 3D grid - data to be used to calculated
                     provenance statistics. If 3D, 1st dimension must be time.
        """
        start_hour, end_hour, records = \
        self.generateProvenanceRecords(prov_path, start_time, data, **kwargs)
        num_days = len(records)
        start_index = self.indexForHour(prov_path, start_hour)
        end_index = start_index + num_days

        dataset = self.getDataset(prov_path)
        names, formats = zip(*dataset.dtype.descr)
        provenance = N.rec.fromrecords(records, shape=(num_days,),
                           formats=list(formats), names=list(names))
        dataset[start_index:end_index] = provenance

        return start_hour, end_hour

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def insertGroupProvenance(self, path, start_time, data1, data2, **kwargs):
        """ Inserts records into a provenance dataset using statistics
        from the input data arrays. It's purpose if to overwrite previously
        stored provenance with statistics from more recent information.

        Arguments
        --------------------------------------------------------------------
        path : string - full path to the group or provenance dataset
        start_time : datetime, scalar - hour/doy of provenance entry. If
                     input data is 3D, it is the first hour/doy and entries
                     will be generated for each day.
        data1, data2 : 2D or 3D numpy arrays - dimensions must match
                       data to be used to calculate provenance statistics.
                       If 3D, time must be the 1st dimension.

        Returns
        --------------------------------------------------------------------
        string containing full path to group provenance dataset


        IMPORTANT
        --------------------------------------------------------------------
        This function does not update the hour attributes >>>
             last_valid_time, last_obs_time, or fcast_end_time,
        Use the "updateGroupProvenance" method when an update of validity
        hour tracking attributes is also required.
        """
        if self.hasDataset(path): prov_path = path
        else: prov_path = '%s.provenance' % path
        provenance = self.generateGroupProvenanceRecords(prov_path,
                                  start_time, data1, data2, **kwargs)
        start_hour, end_hour, records = provenance
        num_hours = len(records)
        start_index = self.indexForHour(prov_path, start_hour)
        end_index = start_index + num_hours

        dataset = self.getDataset(prov_path)
        names, formats = zip(*dataset.dtype.descr)
        provenance = N.rec.fromrecords(records, shape=(num_hours,),
                           formats=list(formats), names=list(names))
        dataset[start_index:end_index] = provenance

        return prov_path, start_hour, end_hour

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def lastValidHour(self, dataset_path, start_time, data, **kwargs):
        if not isinstance(data, N.ndarray):
            errmsg = '"data" argument for "%s" must be a NumPy array.'
            raise TypeError, errmsg % dataset_path

        # always honor a user-specified last valid time
        last_valid_hour = \
            kwargs.get('last_valid_hour', kwargs.get('last_valid_time', None))
        if last_valid_hour is not None: return last_valid_hour

        data_path = dataset_path
        if 'provenance' in dataset_path:
            reference = self.datasetAttribute(dataset_path, 'reference', None)
            if reference is not None: data_path = reference

        # last valid hour not specified by user
        # figure it out from the data
        if data.ndim == 3: # 3D rray is a multiple days
            view = self.datasetAttribute(data_path, 'view')
            # for now, the code assumes a tyx or txy data view
            if view in ('txy','tyx'): 
                num_valid_rows = num_rows = data.shape[0]
                num_nodes = data.shape[1] * data.shape[2]
            elif view in ('xyt','yxt'):
                num_valid_rows = num_rows = data.shape[2]
                num_nodes = data.shape[0] * data.shape[1]
            # starting from the last row in the array
            # find first row with at least one node with a valid value
            row = num_rows-1
            missing, where = \
                self._whereMissing(data[row], dataset=data_path)
            while row > 0 and len(where[0]) == num_nodes:
                # day has no valid values, decrement day counters
                row -= 1
                num_valid_rows -= 1
                missing, where = \
                    self._whereMissing(data[row], missing=missing)

        elif data.ndim == 2: # 2D array is a single hour
            # an array filled with N.nan is not valid
            missing, where = \
                self._whereMissing(data, dataset=data_path)
            if len(where[0]) == data.size:
                num_valid_rows = 0
            else: num_valid_rows = 1

        else: # invalid array
            errmsg = '%dD array is not valid for "%s" dataset.'
            raise ValueError, errmsg % (data.ndim, data_path)

        # last valid hour must not be changed when there are no valid rows
        if num_valid_rows == 0:
            last_valid_hour =  None

        # last valid time will be be extended by number of valid rows
        elif num_valid_rows == 1:
            last_valid_hour = \
                start_time + datetime.timedelta(hours=num_valid_rows-1)
        else:
            last_valid_hour = start_time

        return last_valid_hour

    lastValidTime = lastValidHour

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def provenanceGenerator(self, prov_path):
        key = self.getDatasetAttribute(prov_path, 'generator',
                   self.getDatasetAttribute(prov_path, 'provenance',
                        'default'))
        return self._provenance_generators.get(prov_path,
                                           self._provenance_generators[key])

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def refreshDataset(self, dataset_path, start_time, data, **kwargs):
        """
        CAUTION: The 'refreshDataset' function DOES NOT UPDATE THE
                 DATASET'S TIME ATTRIBUTES. The 'updateDataset'
                 function can be used to update both the dataset
                 and it's associated time attributes.
        """
        time_index = self.indexForTime(dataset_path, start_time, **kwargs)
        num_hours = \
            self._insertTimeSlice(dataset_path, data, time_index, **kwargs)
        if kwargs.get('update_provenance', False):
            prov_path = kwargs.get('provenance_path', 'provenance')
            self.updateProvenance(prov_path, start_time, data, **kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def registerProvenanceGenerator(self, key, generator):
        self._provenance_generators[key] = generator

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def setForecastTimes(self, dataset_path, start_time, end_time, **kwargs):
        last_obs_time = self.timeAttribute(dataset_path, 'last_obs_time')
        if end_time > last_obs_time:
            if start_time <= last_obs_time:
                start_time = last_obs_time + datetime.timedelta(hours=1)

            self.setTimeAttribute(dataset_path, 'fcast_start_time', start_time,
                                  **kwargs)
            self.setTimeAttribute(dataset_path, 'fcast_end_time', end_time,
                                  **kwargs)
            self.setLastValidTime(dataset_path, end_time, 'forecast')
        else:
            errmsg = 'Attmepting to set forecast prior to last observation.'
            errmsg += '\nLast obs time = %s'
            errmsg += '\nForecast start time = %s'
            errmsg += '\nForecast end time = %s'
            times = ( last_obs_time.strftime('%Y-%m-%d:%H'),
                      start_time.strftime('%Y-%m-%d:%H'),
                      end_time.strftime('%Y-%m-%d:%H') )
            raise ValueError, errmsg % times

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def setLastObsTime(self, dataset_path, hour, **kwargs):
        last_obs_time = self.timeAttribute(dataset_path, 'last_obs_time')
        if last_obs_time is None or hour > last_obs_time:
            self.setTimeAttribute(dataset_path, 'last_obs_time', hour,
                                  **kwargs)
            self.setLastValidDate(dataset_path, hour, 'unknown')
            # make adjustments when obs hour stomps on forecast
            self.adjustForecast(dataset_path, hour, **kwargs)

        elif hour < last_obs_time:
            errmsg = 'Attmepting to set last_obs_time prior to current last observation.'
            errmsg += '\nCurrent obs time = %s'
            errmsg += '\nRequested obs time = %s'
            raise ValueError, errmsg % (last_obs_time.strftime('%Y-%m-%d:%H'),
                                        hour.strftime('%Y-%m-%d:%H'))
        # else: do nothing, hour is the same as last_obs_time

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def setLastValidTime(self, dataset_path, hour, source):
        last_valid_time = self.timeAttribute(dataset_path, 'last_valid_time')
        if last_valid_time is None or hour > last_valid_time:
            self.setTimeAttribute(dataset_path, 'last_valid_time', hour)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def setTimeAttribute(self, dataset_path, attribute_name, hour, **kwargs):
        if isinstance(hour, basestring):
            if ':' not in hour:
                errmsg = '"%s" is an invalid value for "%s"'
                raise ValueError, errmsg % (hour, attribute_name)
            self.setObjectAttribute(dataset_path, attribute_name, hour)
        else:
            include_timezone = kwargs.get('include_timezone', False)
            if not tzutils.isInTimezone(hour, self.tzinfo):
                tzhour = tzutils.asHourInTimezone(hour, self.tzinfo)
                hour_str = tzutils.hourAsString(tzhour, include_timezone)
            else: hour_str = tzutils.hourAsString(hour, include_timezone)
            self.setObjectAttribute(dataset_path, attribute_name, hour_str)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def setTimeAttributes(self, dataset_path, include_timezone=False, **hours):
        for attribute_name, hour in hours.items():
            self.setTimeAttribute(dataset_path, attribute_name, hour,
                                  include_timezone=include_timezone)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def setValidationTime(self, path, start_time, end_time, source):
        # dataset period must be 'hour'
        period = self.getDatasetAttribute(path, 'period', None)
        if period != 'hour': return

        # data was either source observations or from a forecast
        if source == 'forecast':
            self.setForecastTimes(path, start_time, end_time, **kwargs)
        else: self.setLastObsTime(path, end_time, **kwargs)
        self.setLastValidTime(path, end_time, source)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def updateDataset(self, dataset_path, start_time, data, **kwargs):
        time_index = self.indexForTime(dataset_path, start_time, **kwargs)
        num_hours = \
            self._insertTimeSlice(dataset_path, data, time_index, **kwargs)
        end_time = start_time + datetime.timedelta(hours=num_hours-1)
        self.setValidationTime(dataset_path, start_time, end_time, **kwargs)
        if kwargs.get('update_provenance', False):
            prov_path = kwargs.get('provenance_path', 'provenance')
            self.updateProvenance(prov_path, start_time, data, **kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def updateGroupProvenance(self, path, start_time, data1, data2, **kwargs):
        """ Inserts records into a provenance dataset using statistics
        from the input data arrays _AND_ updates the appropriate validity
        hour tracking attributes of the provenance dataset: last_valid_time,
        last_obs_time, fcast_end_time

        Arguments
        --------------------------------------------------------------------
        path : string - full path to the group or provenance dataset
        start_time : datetime, scalar - hour/doy of provenance entry. If
                     input data is 3D, it is the first hour/doy and entries
                     will be generated for each day.
        data1, data2 : 2D or 3D numpy arrays - dimensions must match
                       data to be used to calculate provenance statistics.
                       If 3D, time must be the 1st dimension.
        """
        prov_path, start_hour, end_hour = \
            self.insertGroupProvenance(path, start_time, data1, data2, **kwargs)
        self.setValidationTime(prov_path, start_hour, end_hour, **kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def updateProvenance(self, prov_path, start_time, data, **kwargs):
        """ Inserts records into a provenance dataset using statistics
        from the input data array _AND_ updates the appropriate validity
        hour tracking attributes of the provenance dataset: last_valid_time,
        last_obs_time, fcast_end_time

        Arguments
        --------------------------------------------------------------------
        prov_path  : string - path to a provenance dataset.
        start_time : datetime, scalar - hour/doy of provenance entry. If
                     input data is 3D, it is the first hour/doy and entries
                     will be generated for each day.
        data       : 2D or 3D grid - data to be used to calculated
                     provenance statistics. If 3D, 1st dimension must be time.
        """
        start_hour, end_hour = \
            self.insertProvenance(prov_path, start_time, data, **kwargs)
        self.setValidationTime(prov_path, start_hour, end_hour, **kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    #
    # time slicing method overrides
    #
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _insertInto2DView(self, view, dataset, data, time_index):
        unsupported = '"%s" is not a supported 2D view.'
        if data.ndim == 2:
            if view in ('tx','ty'):
                num_hours = data.shape[0]
                end_indx = time_index + num_hours
                dataset[time_index:end_indx,:] = data
            elif view in ('xt','yt'):
                num_hours = data.shape[1]
                end_indx = time_index + num_hours
                dataset[:,time_index:end_indx] = data
            else:
                raise ValueError, unsupported % view
        elif data.ndim == 1:
            num_hours = 1
            if view in ('tx','tx'): dataset[time_index,:] = data
            elif view in ('xt','yt'): dataset[:,time_index] = data
            else:
                raise ValueError, unsupported % view
        else:
            errmsg = 'Cannot insert %sD data into 2D dataset.'
            raise IndexError, errmsg % data.ndim

        return num_hours, dataset

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _insertInto3DView(self, view, dataset, data, time_index):
        unsupported = '"%s" is not a supported 3D view.'
        if data.ndim == 3:
            if view in ('tyx','txy'):
                num_hours = data.shape[0]
                end_indx = time_index + num_hours
                dataset[time_index:end_indx,:,:] = data
            elif view in ('yxt','xyt'):
                num_hours = data.shape[2]
                end_indx = time_index + num_hours
                dataset[:,:,time_index:end_indx] = data
            else:
                raise ValueError, unsupported % view
        elif data.ndim == 2:
            num_hours = 1
            if view in ('tyx','txy'):
                dataset[time_index,:,:] = data
            elif view in ('yxt','xyt'):
                dataset[:,:,time_index] = data
            else:
                raise ValueError, unsupported % view
        else:
            errmsg = 'Cannot insert %sD data into 3D dataset.'
            raise IndexError, errmsg % data.ndim

        return num_hours, dataset

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _insertTimeSlice(self, dataset_path, data, time_index, **kwargs):
        if not isinstance(data, N.ndarray):
            errmsg = 'Data for "%s" dataset must be a NumPy array'
            raise TypeError, errmsg % dataset_path
        dataset = self.getDataset(dataset_path)
        view = dataset.attrs.get('view',None)
        
        errmsg = '"%s" dataset does not support insertion by time slice.'
        assert(view is not None and 't' in view), errmsg % dataset_path

        errmsg = 'Dimension/View shape mismatch in "%s" dataset.'
        dataset_dims = len(dataset.shape)
        if dataset_dims == 3:
            assert(len(view) == 3), errmsg % dataset_path
            data = self._processDataIn(dataset_path, data, **kwargs)
            num_hours, dataset = \
                self._insertInto3DView(view, dataset, data, time_index)
        elif dataset_dims == 2:
            assert(len(view) == 2), errmsg % dataset_path
            data = self._processDataIn(dataset_path, data, **kwargs)
            num_hours, dataset = \
                self._insertInto2DView(view, dataset, data, time_index)
        else:
            errmsg = '"%s" dataset does not support insertion by time slice.'
            raise ValueError, errmsg % dataset_path

        dataset.attrs['updated'] = self.timestamp

        return num_hours

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # private methods
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _loadProvenanceGenerators_(self):
        self._provenance_generators = {
            'default':timeStampProvenanceGenerator,
            'timeaccum':timeAccumStatsProvenanceGenerator,
            'timestamp':timeStampProvenanceGenerator,
            'timestats':timeStatsProvenanceGenerator,
        }

    def _preInitHourlyFileManager_(self, **kwargs):
        self._preInitHourlyFileReader_(**kwargs)
        self._unpackers = { }

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class HourlyGridFileManager(HourlyGridFileManagerMethods,
                            Hdf5HourlyGridFileManager):

    def __init__(self, hdf5_filepath, mode='r', **kwargs):
        self._preInitHourlyFileManager_()
        Hdf5HourlyGridFileManager.__init__(self, hdf5_filepath, mode)

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _loadManagerAttributes_(self):
        Hdf5HourlyGridFileManager._loadManagerAttributes_(self)
        self._loadHourGridAttributes_()
        self._loadProvenanceGenerators_()


