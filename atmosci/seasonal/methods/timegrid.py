
import datetime
from dateutil.relativedelta import relativedelta

import numpy as N

from atmosci.utils.timeutils import asDatetime, asDatetimeDate, dateAsTuple

from atmosci.seasonal.methods.grid import GridFileManagerMethods
from atmosci.seasonal.methods.grid import GridFileReaderMethods

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class TimeGridFileReaderMethods(GridFileReaderMethods):

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def compareDates(self, date1, op, date2):
        if isinstance(date1, datetime.datetime):
            if isinstance(date2, datetime.datetime):
                return self._compareDates(date1, op, date2)
            else: return self._compareDates(date1, op, asDatetime(date2))
        elif isinstance(date1, datetime.date):
            if isinstance(date2, datetime.date):
                return self._compareDates(date1, op, date2)
            else: return self._compareDates(date1, op, asDatetimeDate(date2))
        elif isinstance(date1, (tuple, basestring)):
            return self._compareDates(asDatetimeDate(date1), op, date2)
        else:
            errmsg = 'Invalid type for argument "date1" : %s'
            raise TypeError, errmsg % str(type(date1))

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    """
    def dateAttributes(self, object_path=None, convert=False):
        dates = { }
        if object_path is None:
            attrs = self.fileAttributes(object_path)
        else: attrs = self.objectAttributes(object_path)
        if convert:
            for key in attrs:
                if key.endswith('date'):
                    dates[key] = asDatetimeDate(attrs[key])
        else:
            for key in attrs:
                if key.endswith('date'): dates[key] = attrs[key]
        return dates
    """
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def dataForTime(self, dataset_path, time_obj, lon=None, lat=None,
                          **kwargs):
        time_indx = self._timeToIndex(dataset_path, time_obj)
        if lon is not None:
            y, x = self.ll2index(lon, lat)
            data = self._extractByTimeAtNode(dataset_path, time_indx, x, y)
        else: data = self._extractByTimeIndex(dataset_path, time_indx)
        return self._processDataOut(dataset_path, data, **kwargs)
    getDataForTime = dataForTime

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def sliceAtNode(self, dataset_path, start_time_obj, end_time_obj,
                          lon, lat, **kwargs):
        sindx, eindx = \
        self._timesToIndexes(dataset_path, start_time_obj, end_time_obj)
        y, x = self.ll2index(lon, lat)
        data = self._extractSliceAtNode(dataset_path, sindx, eindx, x, y)
        return self._processDataOut(dataset_path, data, **kwargs)
    getSliceAtNode = sliceAtNode

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def timeSlice(self, dataset_path, start_time_obj, end_time_obj, **kwargs):
        if end_time_obj is not None:
            start_indx, end_indx = \
            self._timesToIndexes(dataset_path, start_time_obj, end_time_obj)
            data = self._extractTimeSlice(dataset_path, start_indx, end_indx)
        else:
            time_indx = self._timeToIndex(dataset_path, start_time_obj)
            data = self._extractByTimeIndex(dataset_path, time_indx)
        return self._processDataOut(dataset_path, data, **kwargs)
    getTimeSlice = timeSlice

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def lastValidTime(self, dataset_path, limit='valid'):
        attrs = self.getDatasetAttribute(dataset_path)
        return attrs('last_%s_%s' % (limit, attrs['period']))

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _dateToIndex(self, dataset_path, time_obj):
        if isinstance(time_obj, datetime.datetime):
            date = datetime.date(*time_obj.timetuple()[:3])
        elif isinstance(time_obj, datetime.date):
            date = time_obj
        elif isinstance(time_obj, (tuple,list)):
            date = self._dateFromSequence(time_obj)
        else:
            errmsg = 'Invalid data type for "time_obj" argument : %s'
            raise TypeError, errmsg % str(type(time_obj))

        attrs = self.getDatasetAttributes(dataset_path)
        start_date = attrs.get('start_date', None)

        if start_date is not None:
            if isinstance(start_date, basestring):
                start_date = asDatetimeDate(start_date)
                end_date = asDatetimeDate(attrs['end_date'])
            else:
                start_date = self._dateFromSequence(start_date)
                end_date = self._dateFromSequence(attrs['end_date'])
        else:
            start_day = attrs.get('start_day', None)
            if start_day is not None:
                start_date = self._dateFromSequence(start_day)
                end_date = self._dateFromSequence(attrs['end_day'])
            else:
                errmsg = '"%s" dataset is not indexable by date.'
                raise IndexError, errmsg % dataset_path

        if date >= start_date and date <= end_date:
            return (date - start_date).days
        else:
            errmsg = '"time_obj" translates to date outside valid range'
            errmsg += ' (%s to %s) for dataset "%s"'
            errmsg = errmsg % (str(start_date), str(end_date), dataset_path)
            raise TypeError, errmsg

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _dateFromSequence(self, time_seq):
        if len(time_seq) == 2: # (month, day)
            if self.target_year == 366: # leap year
                return datetime.date(1972, *time_seq)
            elif self.target_year == 365: # normal year
                return datetime.date(1971, *time_seq)
            else: # index based on day in some other year
                return datetime.date(self.target_year, *time_seq)
        elif len(time_obj) == 3: # (year, month, day)
            return datetime.date(*time_obj)
        else:
            errmsg = 'Invalid size for sequence in "time_obj" argument : %s'

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _doyToIndex(self, dataset_path, time_obj):
        doy_attr = self.getDatasetAttribute(dataset_path, 'doy', None)
        if doy_attr is not None:
            start_doy, end_doy = doy_attr
            if isinstance(time_obj, int):
                doy = time_obj
            elif isinstance(time_obj, (datetime.date, datetime.datetime)):
                doy = time_obj.timetuple().tm_yday
            elif isinstance(time_obj, (tuple,list)):
                date = self._dateFromSequence(time_obj)
                doy = date.timetuple().tm_yday
            else:
                errmsg = 'Invalid type for "time_obj" argument : %s'
                raise TypeError, errmsg % str(type(time_obj))

            if doy < start_doy or doy > end_doy:
                errmsg = '"time_obj" translates to index %d outside valid'
                errmsg += ' DOY range from %d to %d'
                raise IndexError, errmsg % (doy, start_doy, end_day)
            else: return doy - start_doy
        else:
            raise TypeError, '"%s" is not a "doy" dataset' % dataset_path

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _yearToIndex(self, dataset_path, time_obj):
        raise NotImplementedError


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # start/end time indexes
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _timesToIndexes(self, dataset_path, start_time_obj, end_time_obj):
        period = self.getDatasetAttribute(dataset_path, 'period', None)
        if period is not None:
            if period == 'doy':
                indexes = \
                self._doyToIndexes(dataset_path, start_time_obj, end_time_obj)
            elif period in ('date','day'):
                indexes = \
                self._datesToIndexes(dataset_path, start_time_obj, end_time_obj)
            elif period == 'year':
                indexes = \
                self._yearsToIndexes(dataset_path, start_time_obj, end_time_obj)
            else:
                errmsg = '%s does not support indexing by "%s"'
                raise AttributeError, errmsg % (self.__class__.__name__, period)
            return indexes 
        else:
            errmsg = '"%s" dataset is not indexable by time.'
            errmsg += ' It does not have a "period" attribute.'
            raise IndexError, errmsg % dataset_path

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _doyToIndexes(self, dataset_path, start_time_obj, end_time_obj):
        doy_attr = self.getDatasetAttribute(dataset_path, 'doy', None)
        if doy_attr is not None:
            start_doy, end_doy = doy_attr
            if isinstance(start_time_obj, int):
                sdoy = start_time_obj
                edoy = end_time_obj
            elif isinstance(start_time_obj, (datetime.date, datetime.datetime)):
                sdoy = start_time_obj.timetuple().tm_yday
                edoy = end_time_obj.timetuple().tm_yday
            elif isinstance(start_time_obj, (tuple,list)):
                date = self._dateFromSequence(start_time_obj)
                sdoy = date.timetuple().tm_yday
                date = self._dateFromSequence(end_time_obj)
                edoy = date.timetuple().tm_yday
            else:
                errmsg = 'Invalid type for "time_obj" argument : %s'
                raise TypeError, errmsg % str(type(time_obj))

            if sdoy >= start_doy and edoy <= end_doy:
                return (sdoy-start_doy, (edoy-start_doy)+1)
            else: 
                errmsg = '"time_obj" translates to index %d outside valid'
                errmsg += ' DOY range from %d to %d'
                raise IndexError, errmsg % (doy, start_doy, end_day)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _datesToIndexes(self, dataset_path, start_time_obj, end_time_obj):
        attrs = self.getDatasetAttributes(dataset_path)
        start_date = attrs.get('start_date', None)
        if start_date is not None:
            if isinstance(start_date, basestring):
                start_date = asDatetimeDate(start_date)
                end_date = asDatetimeDate(attrs['end_date'])
            else:
                start_date = self._dateFromSequence(start_date)
                end_date = self._dateFromSequence(attrs['end_date'])
        else:
            start_day = attrs.get('start_day', None)
            if start_day is not None:
                start_date = self._dateFromSequence(start_day)
                end_date = self._dateFromSequence(attrs['end_day'])
            else:
                errmsg = '"%s" dataset is not indexable by date.'
                raise IndexError, errmsg % dataset_path

        if isinstance(start_time_obj, datetime.datetime):
            sdate = start_time_obj.date()
            edate = end_time_obj.date()
        elif isinstance(start_time_obj, datetime.date):
            sdate = start_time_obj
            edate = end_time_obj
        elif isinstance(start_time_obj, (tuple,list)):
            sdate = self._dateFromSequence(start_time_obj)
            edate = self._dateFromSequence(end_time_obj)
        elif isinstance(start_time_obj, basestring):
            sdate = asDatetimeDate(start_time_obj)
            edate = asDatetimeDate(end_time_obj)
        else:
            errmsg = 'Invalid data type for time object arguments : %s'
            raise TypeError, errmsg % str(type(start_time_obj))

        if sdate >= start_date and edate <= end_date:
            return ((sdate-start_date).days, (edate-start_date).days+1)
        else:
            errmsg = 'Invalid value in time object arguments : %s, %s'
            raise ValueError, errmsg % (str(start_time_obj), str(end_time_obj))
        
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _yearsToIndexes(self, dataset_path, start_time_obj, end_time_obj):
        raise NotImplementedError


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # extraction by time index
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _extractByTimeAtNode(self, dataset_path, time_indx, x, y):
        dataset = self.getDataset(dataset_path)
        view = dataset.attrs.get('view',None)
        if view == 'txy':
            return dataset[time_indx,x,y]
        elif view == 'tyx':
            return dataset[time_indx,y,x]
        elif view == 'xyt':
            return dataset[time_indx:end_indx]
        elif view == 'yxt':
            return dataset[time_indx:end_indx]
        else:
            errmsg = '"%s" dataset does not support extraction by time slice.'
            raise IndexError, errmsg % dataset_path

    def _extractByTimeIndex(self, dataset_path, time_indx):
        dataset = self.getDataset(dataset_path)
        view = dataset.attrs.get('view',None)
        if view in ('tyx','txy'):
            return dataset[time_indx,:,:]
        elif view in ('yxt','xyt'):
            return dataset[:,:,time_indx]
        else:
            errmsg = '"%s" dataset does not support extraction by time index.'
            raise IndexError, errmsg % dataset_path

    def _extractSliceAtNode(self, dataset_path, start_indx, end_indx, x, y):
        dataset = self.getDataset(dataset_path)
        view = dataset.attrs.get('view',None)
        if view == 'txy':
            return dataset[start_indx:end_indx,x,y]
        elif view == 'tyx':
            return dataset[start_indx:end_indx,y,x]
        elif view == 'xyt':
            return dataset[x,y,start_indx:end_indx]
        elif view == 'yxt':
            return dataset[y,x,start_indx:end_indx]
        else:
            errmsg = '"%s" dataset does not support extraction by time slice.'
            raise IndexError, errmsg % dataset_path

    def _extractTimeSlice(self, dataset_path, start_indx, end_indx):
        dataset = self.getDataset(dataset_path)
        view = dataset.attrs.get('view',None)
        if view in ('tyx','txy'):
            return dataset[start_indx:end_indx,:,:]
        elif view in ('yxt','xyt'):
            return dataset[:,:,start_indx:end_indx]
        else:
            errmsg = '"%s" dataset does not support extraction by time slice.'
            raise IndexError, errmsg % dataset_path


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # time short cuts for datasets
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _datasetEndDate(self, dataset):
        if 'end_date' in dataset.attrs.keys():
            return asDatetime(dataset.end_date)
        elif 'end_day' in attrs:
            return datetime.datetime(self._targetYear(), *attrs['end_day'])
        else: return self._fileEndDate()

    def _datasetEndDay(self, dataset):
        attrs = dict(dataset.attrs)
        if 'end_day' in attrs: return attrs['end_day']
        elif 'end_date' in attrs: return dateAsTuple(attrs['end_date'])[1:]
        else: return self._fileEndDay()

    def _datasetEndDoy(self, dataset):
        attrs = dict(dataset.attrs)
        if hasattr(self, 'end_doy'): return int(attrs['end_doy'])
        elif 'end_day' in attrs:
            date = datetime.date(self._targetYear(), *attrs['end_day'])
            return date.timetuple().tm_yday
        elif 'end_date' in attrs:
            return asDatetime(attrs['end_date']).timetuple().tm_yday
        else: return self._fileEndDoy()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _datasetStartDate(self, dataset):
        if 'start_date' in dataset.attrs.key():
            return asDatetime(dataset.start_date)
        else: return self._fileStartDate()

    def _datasetStartDay(self, dataset):
        attrs = dict(dataset.attrs)
        if 'start_day' in attrs: return attrs['start_day']
        elif 'start_date' in attrs: return dateAsTuple(attrs['start_date'])[1:]
        else: return self._fileStartDay()

    def _datasetStartDoy(self, dataset):
        attrs = dict(dataset.attrs)
        if hasattr(self, 'start_doy'): return int(attrs['start_doy'])
        elif 'start_day' in attrs:
            date = datetime.date(self._targetYear(), *attrs['start_day'])
            return date.timetuple().tm_yday
        elif 'start_date' in attrs:
            return asDatetime(attrs['start_date']).timetuple().tm_yday
        else: return self._fileStartDoy()


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # time short cuts for file
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _fileEndDate(self):
        if hasattr(self, 'end_date'): return self.end_date
        elif hasattr(self, 'end_day'):
            return datetime.datetime(self._targetYear(), *self.end_day)
        raise IndexError, 'end date not available for this file.'

    def _fileEndDay(self):
        if hasattr(self, 'end_day'): return self.end_day
        elif hasattr(self, 'end_date'):
            return (self.end_date.month, self.end_date.day)
        raise IndexError, 'end day not available for this file.'

    def _fileEndDoy(self):
        if hasattr(self, 'end_doy'): return self.end_doy
        elif hasattr(self, 'end_day'):
            date = datetime.date(self._targetYear(), *self.end_day)
            return date.timetuple().tm_yday
        elif hasattr(self, 'end_date'):
            return self.end_date.timetuple().tm_yday
        raise IndexError, 'end d.o.y not available for this file.'

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _fileStartDate(self):
        if hasattr(self, 'start_date'): return self.start_date
        raise IndexError, 'start date not available for this file.'

    def _fileStartDay(self):
        if hasattr(self, 'start_day'): return self.start_day
        elif hasattr(self, 'start_date'):
            return (self.start_date.month, self.start_date.day)
        raise IndexError, 'start day not available for this file.'

    def _fileStartDoy(self):
        if hasattr(self, 'start_doy'): return self.start_doy
        elif hasattr(self, 'start_day'):
            date = datetime.date(self._targetYear(), *self.start_day)
            return date.timetuple().tm_yday
        elif hasattr(self, 'start_date'):
            return self.start_date.timetuple().tm_yday
        raise IndexError, 'start d.o.y not available for this file.'

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _compareDates(self, date1, op, date2):
        if   op == '==': return date1 == date2
        elif op == '<':  return date1 < date2
        elif op == '<=': return date1 <= date2
        elif op == '>':  return date1 > date2
        elif op == '>=': return date1 >= date2
        elif op == '!=': return date1 != date2
        else: raise ValueError, 'Unsupported operator : "%s"' % op

    def _targetYear(self):
        if self.target_year == 366: return 1972
        elif self.target_year == 365: return 1971
        else: return self.target_year

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _loadProjectFileAttributes_(self):
        attrs = self.getFileAttributes()
        if not hasattr(self, 'target_year'):
            self.target_year = attrs.get('target_year', None)
            if self.target_year is not None:
                self.target_year = int(self.target_year)
        if not hasattr(self, 'start_date'):
            self.start_date = attrs.get('start_date', None)
        if not hasattr(self, 'end_date'):
            self.end_date = attrs.get('end_date', None)

        for attr_name, attr_value in attrs.items():
            if attr_name not in ('start_date', 'end_date', 'target_year'):
                self.__dict__[attr_name] = attr_value

        self._configDatasets_()
        self._loadDatasetAttrs_()
        self._postLoadFileAttrs_()


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class TimeGridFileManagerMethods(TimeGridFileReaderMethods,
                                 GridFileManagerMethods):

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    #
    # generic time series data methods
    #
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def insertTimeSlice(self, dataset_path, start_time_obj, data, **kwargs):
        start_indx = self._timeToIndex(dataset_path, start_time_obj)
        end_indx = self._insertTimeSlice(dataset_path, start_indx, data)
        return end_indx - start_indx

    def insertByTime(self, dataset_path, time_obj, data, **kwargs):
        time_indx = self._timeToIndex(dataset_path, time_obj)
        self._insertByTimeIndex(dataset_path, time_indx, data)
        return 1

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def refreshDataset(self, dataset_path, start_date, data, **kwargs):
        if data.ndim == 3:
            span = \
                self.insertTimeSlice(dataset_path, start_date, data, **kwargs)
        else: 
            span = self.insertByTime(dataset_path, start_date, data, **kwargs)
        return self._endTimeFromSpan(dataset_path, start_date, span)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def updateDataset(self, dataset_path, start_date, data, **kwargs):
        end_date = \
            self.refreshDataset(dataset_path, start_date, data, **kwargs)
        self.setValidationDate(dataset_path, start_date, data, **kwargs)
        return end_date

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    #
    # generic provenance generators
    #
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def generateProvenanceRecords(self, prov_path, start_date, data, **kwargs):
        """ Generates provenance records based on statistics from a single
        data array.

        Arguments
        --------------------------------------------------------------------
        prov_path  : string - path to a provenance dataset.
        start_date : datetime, scalar - date/doy of provenance entry. If
                     input data is 3D, it is the first date/doy and entries
                     will be generated for each day.
        data       : 2D or 3D numpy array - data to be used to calculate
                     provenance statistics. If 3D, time must be the 1st
                     dimension.
        """
        prov_key = self.getDatasetAttribute(prov_path, 'key', 'stats')
        generate = self._getRegisteredFunction('generators.%s' % key)
        timestamp = kwargs.get('timestamp', self.timestamp)

        if data_.ndim == 2:
            return [generate(start_date, timestamp, data),]
        else:
            records = [ ]
            num_days = data.shape[0]
            for day in range(num_days):
                date = start_date + relativedelta(days=day)
                record = generateProvenance(date, timestamp, data[day])
                records.append(record)
            return records

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def generateGroupProvenanceRecords(self, prov_path, start_date, data_1,
                                             data_2, **kwargs):
        """ Generates provenance records for groups based on statistics
        from 2 data arrays.

        Arguments
        --------------------------------------------------------------------
        prov_path  : string - path to a provenance dataset.
        start_date : datetime, scalar - date/doy of provenance entry. If
                     input data is 3D, it is the first date/doy and entries
                     will be generated for each day.
        data_1, data_2 : 2D or 3D numpy arrays - dimensions must match
                         data to be used to calculate provenance statistics.
                         If 3D, time must be the 1st dimension.
        """
        prov_key = self.getDatasetAttribute(prov_path, 'key', 'stats')
        gen_key = self.getDatasetAttribute(prov_path, 'generator', prov_key)
        generate = self._getRegisteredFunction('generators.%s' % gen_key)

        timestamp = kwargs.get('timestamp', self.timestamp)

        if data_1.ndim == 2:
            return [generate(start_date, timestamp, data_1, data_2),]
        else:
            records = [ ]
            for day in range(data_1.shape[0]):
                date = start_date + relativedelta(days=day)
                period = data_1[day]
                accum = data_2[day]
                record = generate(date, timestamp, period, accum)
                records.append(record)
            return records

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    #
    # generic provenance update methods
    #
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def insertProvenance(self, prov_path, start_date, data, **kwargs):
        """ Inserts records into a provenance dataset using statistics
        from the input data array. It's purpose if to overwrite previously
        stored provenance with statistics from more recent information.

        NOTE: this function does not update the date attributes >>>
              last_valid_date, last_obs_date, or last_forecast_date,
              Use the "updateProvenance" method when an updates of
              validity date tracking attributes is also required.

        Arguments
        --------------------------------------------------------------------
        prov_path  : string - path to a provenance dataset.
        start_date : datetime, scalar - date/doy of provenance entry. If
                     input data is 3D, it is the first date/doy and entries
                     will be generated for each day.
        data       : 2D or 3D grid - data to be used to calculated
                     provenance statistics. If 3D, 1st dimension must be time.
        """
        records = self.generateProvenanceRecords(prov_path, start_date,
                                                 data, **kwargs)
        num_days = len(records)
        start_index = self.indexFromDate(prov_path, start_date)
        end_index = start_index + num_days

        dataset = self.getDataset(prov_path)
        names, formats = zip(*dataset.dtype.descr)
        provenance = N.rec.fromrecords(records, shape=(num_days,),
                           formats=list(formats), names=list(names))
        dataset[start_index:end_index] = provenance
        return num_days

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def insertGroupProvenance(self, path, start_date, data_1, data_2, **kwargs):
        """ Inserts records into a provenance dataset using statistics
        from the input data arrays. It's purpose if to overwrite previously
        stored provenance with statistics from more recent information.

        Arguments
        --------------------------------------------------------------------
        path : string - full path to the group or provenance dataset
        start_date : datetime, scalar - date/doy of provenance entry. If
                     input data is 3D, it is the first date/doy and entries
                     will be generated for each day.
        data_1, data_2 : 2D or 3D numpy arrays - dimensions must match
                         data to be used to calculate provenance statistics.
                         If 3D, time must be the 1st dimension.

        Returns
        --------------------------------------------------------------------
        string containing full path to group provenance dataset


        IMPORTANT
        --------------------------------------------------------------------
        This function does not update the date attributes >>>
             last_valid_date, last_obs_date, or last_forecast_date,
        Use the "updateGroupProvenance" method when an update of validity
        date tracking attributes is also required.
        """
        if self.hasDataset(path): prov_path = path
        else: prov_path = '%s.provenance' % path
        records = self.generateGroupProvenanceRecords(prov_path, start_date,
                                                      data_1, data_2, **kwargs)
        num_days = len(records)
        start_index = self.indexFromDate(prov_path, start_date)
        end_index = start_index + num_days

        dataset = self.getDataset(prov_path)
        names, formats = zip(*dataset.dtype.descr)
        provenance = N.rec.fromrecords(records, shape=(num_days,),
                           formats=list(formats), names=list(names))
        dataset[start_index:end_index] = provenance

        return prov_path, num_days

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    #@property
    #def timestamp(self):
    #    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def updateProvenance(self, path, start_date, data, **kwargs):
        """ Inserts records into a provenance dataset using statistics
        from the input data array _AND_ updates the appropriate validity
        date tracking attributes of the provenance dataset: last_valid_date,
        last_obs_date, last_forecast_date

        Arguments
        --------------------------------------------------------------------
        prov_path  : string - path to a provenance dataset.
        start_date : datetime, scalar - date/doy of provenance entry. If
                     input data is 3D, it is the first date/doy and entries
                     will be generated for each day.
        data       : 2D or 3D grid - data to be used to calculated
                     provenance statistics. If 3D, 1st dimension must be time.
        """
        span = self.insertProvenance(prov_path, start_date, data, **kwargs)
        self.setValidationDate(prov_path, start_date, data, **kwargs)
        return self._endTimeFromSpan(prov_path, start_date, span)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def updateGroupProvenance(self, path, start_date, data_1, data_2,
                                    **kwargs):
        """ Inserts records into a provenance dataset using statistics
        from the input data arrays _AND_ updates the appropriate validity
        date tracking attributes of the provenance dataset: last_valid_date,
        last_obs_date, last_forecast_date

        Arguments
        --------------------------------------------------------------------
        path : string - full path to the group or provenance dataset
        start_date : datetime, scalar - date/doy of provenance entry. If
                     input data is 3D, it is the first date/doy and entries
                     will be generated for each day.
        data_1, data_2 : 2D or 3D numpy arrays - dimensions must match
                         data to be used to calculate provenance statistics.
                         If 3D, time must be the 1st dimension.
        """
        prov_path, span = self.insertGroupProvenance(path, start_date, data_1,
                                                     data_2, **kwargs)
        self.setValidationDate(prov_path, start_date, data_1, **kwargs)
        return self._endTimeFromSpan(path, start_date, span)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    #
    # validation date methods
    #
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def adjustForecastStartDate(self, dataset_path, obs_date):
        """ Adjust forecast dates in reponse to update of observations

        Arguments
        --------------------------------------------------------------------
        dataset_path : full dot path for dataset to update
        obs_date     : datetime.date representing date of last observation
        """
        fcast_start_date = \
            self.getAttributeAsDate(dataset_path, 'fcast_start_date')

        # check to see if obs date stomped on forecast start date
        if fcast_start_date is not None and fcast_start_date <= obs_date:
            # stomped on forecast start date, check against forecast end date
            fcast_end_date = \
                self.getAttributeAsDate(dataset_path, 'fcast_end_date')
            # if forecast ends after new obs date adjust forecast start date
            if fcast_end_date > obs_date:
                fcast_start_date = obs_date + relativedelta(days=1)
                self.setDateAttribute(dataset_path, 'fcast_start_date',
                                     fcast_start_date)
            # forecast was completely overwritten by observation data
            else:
                # delete forecast date attributes
                #!TODO probably should figure out a better way to indicate
                #      that there is no forecast data
                self.deleteDatasetAttribute(dataset_path, 'fcast_start_date')
                self.deleteDatasetAttribute(dataset_path, 'fcast_end_date')

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def adjustLastObsDate(self, dataset_path, fcast_start_date):
        """ Adjust observation date in response to changes in forecast

        Arguments
        --------------------------------------------------------------------
        dataset_path     : full dot path for dataset to update
        fcast_start_date : datetime.date representing date of last observation
        """
        last_obs_date = \
            self.getAttributeAsDate(dataset_path, 'last_obs_date')

        # check to see if forecast start date stomped on obs date
        if last_obs_date is not None and last_obs_date >= fcast_start_date:
            # stomped on last obs date
            obs_date = fcast_start_date - relativedelta(days=1)
            first_date = self.getAttributeAsDate('__file__', 'start_date')
            if obs_date >= first_date:
                self.setDateAttribute(dataset_path, 'last_obs_date', obs_date)
            else:
                # delete last obs date attribute
                #!TODO probably should figure out a better way to indicate
                #      that there is no observation data
                self.deleteDatasetAttribute(dataset_path, 'last_obs_date')

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def getAttributeAsDate(self, path, attribute_name):
        """ Access a date attribute and return it as a datetime.date
        """
        date_str = self.getObjectAttribute(path, attribute_name, None)
        if date_str is not None: return asDatetimeDate(date_str)
        return None

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def getLastValidDate(self, dataset_path, start_date, data, **kwargs):
        # always honor a user-specified last valid date
        last_valid_date = kwargs.get('last_valid_date', None)
        # last valid date not specified by user, figure it out from the data
        if last_valid_date is None:
            if data.ndim == 2: # 2D rray is a single day
                # an array filled with N.nan is not valid
                missing, where = \
                    self._whereMissing(data, dataset=dataset_path)
                if len(where[0]) == data.size:
                    num_valid_days = 0
                else: num_valid_days = 1

            else: # 3D array
                # for now, the code assumes a zyx or zxy data view 
                num_nodes = data[0].size
                num_valid_days = num_days = data.shape[0]
                # starting from the last day in the array
                # find first day with at least one node with a valid value
                day = num_days-1
                missing, where = \
                    self._whereMissing(data[day], dataset=dataset_path)
                while day > 0 and len(where[0]) == num_nodes:
                    # day has no valid values, decrement day counters
                    day -= 1
                    num_valid_days -= 1
                    missing, where = \
                        self._whereMissing(data[day], missing=missing)

            if num_valid_days == 0:
                # last valid date will not be changed because there are
                # no valid values in the data
                prev_date_str = self.getDatasetAttribute(dataset_path,
                                     'last_valid_date', None)
                if prev_date_str is not None:
                    return asDatetimeDate(prev_date_str)
                else: return None

            # last valid date will be start_date plus number of valid days
            elif num_valid_days == 1:
                last_valid_date = start_date
            else: # num_valid_days > 1
                last_valid_date = \
                    start_date + relativedelta(days=num_valid_days-1)

        return last_valid_date

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def setDateAttribute(self, dataset_path, attribute_name, date):
        if isinstance(date, basestring):
            self.setObjectAttribute(dataset_path, attribute_name, date)
        else:
            date_str = date.strftime('%Y-%m-%d')
            self.setObjectAttribute(dataset_path, attribute_name, date_str)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def setDateAttributes(self, dataset_path, **dates):
        for attribute_name, date in dates.items():
            self.setDateAttribute(dataset_path, attribute_name, date)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def setForecastDates(self, dataset_path, start_date, end_date):
        self.setDateAttribute(dataset_path, 'fcast_start_date', start_date)
        self.setDateAttribute(dataset_path, 'fcast_end_date', end_date)

        # this should NEVER happen, but just in case ....
        # make adjustments when forecast stomps on obs date
        self.adjustLastObsDate(dataset_path, start_date)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def setLastObsDate(self, dataset_path, date, attr_name='last_obs_date'):
        self.setDateAttribute(dataset_path, attr_name, date)
        # make adjustments when obs date stomps on forecast
        self.adjustForecastStartDate(dataset_path, date)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def setValidationDate(self, path, start_date, data, **kwargs):
        # dataset period must be 'date'
        period = self.getDatasetAttribute(path, 'period', None)
        if period != 'date': return

        # only works when there is valid data
        last_date = self.getLastValidDate(path, start_date, data, **kwargs)
        if last_date is None: return

        # update last valid date
        self.setDateAttribute(path, 'last_valid_date', last_date)

        # data was either source observations or from a forecast
        is_forecast = kwargs.get('forecast', False)
        if is_forecast:
            self.setForecastDates(dataset_path, start_date, last_date)
        else:
            self.setLastObsDate(path, last_date)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def setValidationDates(self, paths, start_date, data, **kwargs):
        is_forecast = kwargs.get('forecast', False)

        # dataset period must be 'date'
        period = self.getDatasetAttribute(paths[0], 'period', None)
        if period == 'date':
            # only works when there is valid data
            last_date = \
                self.getLastValidDate(paths[0], start_date, data, **kwargs)
            if last_date is not None:
                for path in paths:
                    # data was either observations or forecast
                    if is_forecast:
                        self.setForecastDates(path, start_date, last_date)
                    else:
                        self.setDateAttribute(path, 'last_obs_date', last_date)
                    # either way, the last available date is the same
                    self.setDateAttribute(path, 'last_valid_date', last_date)


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _dataEndTime(self, dataset_path, time_obj, data):
        period = self.getDatasetAttribute(dataset_path, 'period', None)
        if period is not None:
            ndims = len(data.shape)
            if ndims == 3: span = data.shape[0]
            elif ndims == 2: span = 1
            else:
                #TODO: figure out a better solution here
                return None

            if span == 1: return time_obj
            if period in ('date','day'):
                return time_obj + datetime.timedelta(days=span-1)
            elif period in ('doy','year'):
                return time_obj + span - 1
            else:
                errmsg = '%s does not support indexing by "%s"'
                class_name = self.__class__.__name__
                raise AttributeError, errmsg % (class_name, period)
        else:
            errmsg = '"%s" dataset is not indexable by time.'
            errmsg += ' It does not have a "period" attribute.'
            raise IndexError, errmsg % dataset_path

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _endTimeFromSpan(self, dataset_path, time_obj, span):
        period = self.getDatasetAttribute(dataset_path, 'period', None)
        if period is not None:
            if span == 1: return time_obj
            if period in ('date','day'):
                return time_obj + datetime.timedelta(days=span-1)
            elif period in ('doy','year'):
                return time_obj + span - 1
            elif period == 'hour':
                return time_obj + datetime.timedelta(hours=span-1)
            else:
                errmsg = '%s does not support indexing by "%s"'
                class_name = self.__class__.__name__
                raise AttributeError, errmsg % (class_name, period)
        else:
            errmsg = '"%s" dataset is not indexable by time.'
            errmsg += ' It does not have a "period" attribute.'
            raise IndexError, errmsg % dataset_path

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _timeToIndex(self, dataset_path, time_obj):
        period = self.getDatasetAttribute(dataset_path, 'period', None)
        if period is not None:
            if period == 'doy':
                return self._doyToIndex(dataset_path, time_obj)
            elif period in ('date','day'):
                return self._dateToIndex(dataset_path, time_obj)
            elif period == 'year':
                return self._yearToIndex(dataset_path, time_obj)
            else:
                errmsg = '%s does not support indexing by "%s"'
                class_name = self.__class__.__name__
                raise AttributeError, errmsg % (class_name, period)
        else:
            errmsg = '"%s" dataset is not indexable by time.'
            errmsg += ' It does not have a "period" attribute.'
            raise IndexError, errmsg % dataset_path


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    #
    # time slicing method overrides
    #
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _insertTimeSlice(self, dataset_path, start_indx, data, **kwargs):
        dataset = self.getDataset(dataset_path)
        view = dataset.attrs.get('view',None)
        if data.ndim == 3:
            if view in ('tyx','txy'):
                end_indx = start_indx + data.shape[0]
                dataset[start_indx:end_indx,:,:] = \
                    self._processDataIn(dataset_path, data, **kwargs)
            elif view in ('yxt','xyt'):
                end_indx = start_indx + data.shape[2]
                dataset[:,:,start_indx:end_indx] = \
                    self._processDataIn(dataset_path, data, **kwargs)
        elif data.ndim == 2:
            end_indx = start_indx
            if view in ('tyx','txy'):
                dataset[start_indx,:,:] = \
                    self._processDataIn(dataset_path, data, **kwargs)
            elif view in ('yxt','xyt'):
                dataset[:,:,start_indx] = \
                    self._processDataIn(dataset_path, data, **kwargs)
        else:
            errmsg = '"%s" dataset does not support insertion by time slice.'
            raise IndexError, errmsg % dataset_path

        dataset.attrs['updated'] = self.timestamp
        return end_indx

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _insertByTimeIndex(self, dataset_path, time_indx, data, **kwargs):
        dataset = self.getDataset(dataset_path)
        view = dataset.attrs.get('view',None)
        if view in ('tyx','txy'):
            dataset[time_indx,:,:] = \
                    self._processDataIn(dataset_path, data, **kwargs)
        elif view in ('yxt','xyt'):
            dataset[:,:,time_indx] = \
                    self._processDataIn(dataset_path, data, **kwargs)
        else:
            errmsg = '"%s" dataset does not support insertion by time index.'
            raise IndexError, errmsg % dataset_path

        dataset.attrs['updated'] = self.timestamp
        return time_indx

