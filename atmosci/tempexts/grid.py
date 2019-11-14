
import datetime

import numpy as N

from atmosci.hdf5.dategrid import Hdf5DateGridFileBuilder

from atmosci.seasonal.methods.grid import hdf5ReaderPatch
from atmosci.seasonal.methods.timegrid import TimeGridFileManagerMethods
from atmosci.seasonal.methods.builder  import TimeGridFileBuildMethods

from atmosci.seasonal.grid import SeasonalGridFileReader, \
                                  SeasonalGridFileManager

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class TempextAccessMethods:
    """ Temperature extremes data access methods

    REQUIRED:
        1. must be included in a class derived from
           atmosci.hdf5.manager import Hdf5DateGridFileReader
        2. the derived class must also include
           atmosci.seasonal.methods.TimeGridFileReaderMethods
    """

    def tempExtremes(self, start_date, end_date, **kwargs):
        temp_group = self._tempGroupPath(**kwargs)
        path = '%s.mint' % temp_group
        mint = self.getTimeSlice(path, start_date, end_date, **kwargs)
        path = '%s.maxt' % temp_group
        maxt = self.getTimeSlice(path, start_date, end_date, **kwargs)
        return mint, maxt
    getTempExtremes = tempExtremes

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _tempGroupPath(self, **kwargs):
        group_name = kwargs.get('group_name', 'temps')
        return kwargs.get('group_path', group_name)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _dateToIndex(self, dataset_path, date_obj):
        if isinstance(date_obj, datetime.datetime): date = date_obj.date()
        elif isinstance(date_obj, datetime.date): date = date_obj
        else:
            errmsg = 'Invalid type for "date_obj" argument : %s'
            raise TypeError, errmsg % str(type(date_obj))

        start_date = self.dateAttribute(dataset_path, 'start_date')
        end_date = self.dateAttribute(dataset_path, 'end_date')

        if date >= start_date and date <= end_date:
            return (date - start_date).days
        else:
            errmsg = '%s is outside the valid range' % str(date)
            errmsg = '%s (%%s to %%s) for "%%s" dataset' % errmsg
            errmsg = errmsg % (str(start_date), str(end_date), dataset_path)
            raise ValueError, errmsg
    indexForDate = _dateToIndex
    indexFromDate = _dateToIndex # compatibility with hdf5/dategrid.py
    _indexForDate = _dateToIndex # compatibility with hdf5/dategrid.py


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class TempextUpdateMethods(TempextAccessMethods):
    """ Temperature extremes data update methods
 
    REQUIRED:
        1. must be included in a class derived from
           atmosci.hdf5.manager import Hdf5DateGridFileManager
        2. the derived class must also include
           atmosci.seasonal.methods.TimeGridFileManagerMethods
    """

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def insertTempData(self, temp_group, temp_dsname, start_date, data):
        path = '%s.%s' % (temp_group, temp_dsname)

        self.open('a')
        date_index = self.indexForDate(path, start_date)
        if len(data.shape) == 3:
            end_index = date_index + data.shape[0]
        else : end_index = date_index
        dataset = self.getDataset(path)

        if end_index <= dataset.shape[0]:
            if end_index == date_index:
                dataset[date_index] = data
            else:
                dataset[date_index:end_index] = data
        else:
            dataset[date_index:] = data
        
        missing_value = self.getDatasetAttribute(path, 'missing')
        self.close()

        return path, missing_value

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def insertTempExtremes(self, start_date, mint, maxt, **kwargs):
        """ Inserts data into the mint and maxt datasets of the
        temperature group. It's purpose if to overwrite previously
        stored data with more recent observations.

        Arguments
        --------------------------------------------------------------------
        start_date : datetime, scalar - date/doy of provenance entry. If
                     input data is 3D, it is the first date/doy and entries
                     will be generated for each day.
        mint, maxt : 2D or 3D grid(s) - min & max temperatures to be used to
                     calculated provenance statistics. If 3D, 1st dimension
                     must be time.

        Returns
        --------------------------------------------------------------------
        tuple of strings containing the full path to the datasets
            (mint path, maxt path)


        IMPORTANT
        --------------------------------------------------------------------
        This function does not update the date attributes >>>
             last_valid_date, last_obs_date, or last_forecast_date,
        Use the "updateTempData" method when an update of validity date
        tracking attributes is also required.
        """
        temp_group = self._tempGroupPath(**kwargs)
        print 'insertTempExtremes :', temp_group, start_date, mint.shape, maxt.shape

        mint_path, missing = self.insertTempData(temp_group, 'mint', start_date, mint)
        maxt_path, missing = self.insertTempData(temp_group, 'maxt', start_date, maxt)

        return mint_path, maxt_path, missing

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def insertTempProvenance(self, start_date, mint, maxt, source_tag,
                                   **kwargs):
        """Inserts provenance statistics for time span using data from the
        min and max temperature grids.  It's purpose if to overwrite
        previously stored provenance with statistics from more recent
        information.

        Arguments
        --------------------------------------------------------------------
        start_date : datetime, scalar - date/doy of provenance entry. If
                     input data is 3D, it is the first date/doy and entries
                     will be generated for each day.
        mint, maxt : 2D or 3D grid(s) - min & max temperatures to be used to
                     calculated provenance statistics. If 3D, 1st dimension
                     must be time.
        source_tag : string or list/tuple of strings - the tag name for the
                     source where the temps were acquired. If it is a string,
                     the same tag is applied to all records in this
                     transaction. If a list/tuple, it must contain one tag
                     for each item in the time dimension of the temperature
                     arrays.

        Returns
        --------------------------------------------------------------------
        string containing full path to temperature group provenance dataset


        IMPORTANT
        --------------------------------------------------------------------
        This function does not update the date attributes >>>
             last_valid_date, last_obs_date, or last_forecast_date,
        Use the "updateTempProvenance" method when an update of validity
        date tracking attributes is also required.
        """
        temp_group = self._tempGroupPath(**kwargs)
        prov_path = '%s.provenance' % temp_group

        self.open('a')
        start_index = self.indexFromDate(prov_path, start_date)
        timestamp = kwargs.get('timestamp', self.timestamp)

        dataset = self.getDataset(prov_path)
        key = dataset.attrs.get('key', 'stats')
        generate = self._getRegisteredFunction('generators.%s' % key)

        if mint.ndim == 2:
            num_days = 1
            record = generate(start_date, timestamp, mint, maxt, source_tag)
            records = [record,]
        else:
            records = [ ]
            num_days = mint.shape[0]
            if isinstance(source_tag, (tuple,list)):
                for day in range(num_days):
                    date = start_date + datetime.timedelta(days=day)
                    record = generate(date, timestamp, mint[day], maxt[day],
                                      source_tag[day])
                    records.append(record)
            else: # source is a single string
                for day in range(num_days):
                    date = start_date + datetime.timedelta(days=day)
                    record = generate(date, timestamp, mint[day], maxt[day],
                                      source_tag)
                    records.append(record)
        end_index = start_index + num_days
        names, formats = zip(*dataset.dtype.descr)
        provenance = N.rec.fromrecords(records, shape=(num_days,),
                           formats=list(formats), names=list(names))
        dataset[start_index:end_index] = provenance

        self.close()
        return prov_path

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def refreshTempGroup(self, start_date, mint, maxt, source_tag, **kwargs):
        """Inserts mint, maxt and provenance statistics It's purpose
        is to overwrite previously stored temperature data with more
        recent information.

        Arguments
        --------------------------------------------------------------------
        start_date : datetime, scalar - date/doy of provenance entry. If
                     input data is 3D, it is the first date/doy and entries
                     will be generated for each day.
        mint, maxt : 2D or 3D grid(s) - min & max temperatures to be used to
                     calculated provenance statistics. If 3D, 1st dimension
                     must be time.
        source_tag : string or list/tuple of strings - the tag name for the
                     source where the temps were acquired. If it is a string,
                     the same tag is applied to all records in this
                     transaction. If a list/tuple, it must contain one tag
                     for each item in the time dimension of the temperature
                     arrays.

        IMPORTANT
        --------------------------------------------------------------------
        This function does not update the validity date attributes >>>
             last_valid_date, last_obs_date, or last_forecast_date,
        Use the "updateTempGroupe" method when an update of validity
        date tracking attributes is also required.
        """
        print self.insertTempData(start_date, mint, maxt, **kwargs)
        print self.insertTempProvenance(start_date, mint, maxt, source_tag, **kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def refreshTempProvenance(self, start_date, end_date, source_tag, **kwargs):
        group = self._tempGroupPath(**kwargs)

        path = '%s.mint' % group
        missing_value = self.getDatasetAttribute(path, 'missing')
        mint = self.timeSlice(path, start_date, end_date, **kwargs)
        mint = mint.astype(float)
        mint[N.where(mint == missing_value)] = N.nan
        
        path = '%s.maxt' % group
        missing_value = self.getDatasetAttribute(path, 'missing')
        maxt = self.timeSlice(path, start_date, end_date, **kwargs)
        maxt = maxt.astype(float)
        maxt[N.where(maxt == missing_value)] = N.nan

        self.insertTempProvenance(start_date, mint, maxt,
                                              source_tag, **kwargs)


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def updateTempGroup(self, start_date, mint, maxt, source_tag, **kwargs):
        """Inserts data into the mint, maxt and provenance datasets
        of the temperature group AND updates the appropriate validity
        date tracking attributes : last_valid_date, last_obs_date,
        last_forecast_date

        Arguments
        --------------------------------------------------------------------
        start_date : datetime, scalar - date/doy of provenance entry. If
                     input data is 3D, it is the first date/doy and entries
                     will be generated for each day.
        mint, maxt : 2D or 3D grid(s) - min & max temperatures to be used to
                     calculated provenance statistics. If 3D, 1st dimension
                     must be time.
        source_tag : string or list/tuple of strings - the tag name for the
                     source where the temps were acquired. If it is a string,
                     the same tag is applied to all records in this
                     transaction. If a list/tuple, it must contain one tag
                     for each item in the time dimension of the temperature
                     arrays.
        """

        print '\nupdateTempGroup :'
        if self.debug:
            print '     missing data :', N.where(mint == -32768)

        mint_path, maxt_path, missing_value = \
            self.insertTempExtremes(start_date, mint, maxt, **kwargs)
        
        mint = mint.astype(float)
        mint[N.where(mint == missing_value)] = N.nan

        maxt = maxt.astype(float)
        maxt[N.where(maxt == missing_value)] = N.nan

        prov_path = self.insertTempProvenance(start_date, mint, maxt,
                                              source_tag, **kwargs)
        self.open('a')
        self.setValidationDates( (prov_path, mint_path, maxt_path),
                                  start_date, mint, **kwargs)
        self.close()


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class TemperatureFileReader(TempextAccessMethods, SeasonalGridFileReader):

    def __init__(self, filepath, registry):
        SeasonalGridFileReader.__init__(self, filepath, registry)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class TemperatureFileManager(TempextUpdateMethods, SeasonalGridFileManager):

    def __init__(self, filepath, registry, mode='r', **kwargs):
        SeasonalGridFileManager.__init__(self, filepath, registry, mode)
        self.debug = kwargs.get('debug', False)
        self.verbose = kwargs.get('verbose', self.debug)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class TemperatureFileBuilder(TempextUpdateMethods,
                             TimeGridFileManagerMethods,
                             TimeGridFileBuildMethods,
                             Hdf5DateGridFileBuilder):

    def __init__(self, filepath, registry, config, target_year, source,
                       region, start_date, end_date, **kwargs):
        self._preInitProject_(registry)
        self.preInitBuilder(config, 'tempexts', source, target_year, region,
                            **kwargs)
        Hdf5DateGridFileBuilder.__init__(self, filepath, start_date, end_date,
                                               None, None)
        hdf5ReaderPatch(self)

        self.debug = kwargs.get('debug', False)
        self.verbose = kwargs.get('verbose', self.debug)

        self.initFileAttributes(**kwargs)
        self.postInitBuilder(**kwargs)
        self.close()

