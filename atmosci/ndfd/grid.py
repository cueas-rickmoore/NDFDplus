
import datetime
ONE_HOUR = datetime.timedelta(hours=1)

import numpy as N

from atmosci.utils.config import ConfigObject
from atmosci.utils import tzutils

from atmosci.hdf5.hourgrid import Hdf5HourlyGridFileReader, \
                                  Hdf5HourlyGridFileManager

from atmosci.hourly.grid import HourlyGridFileReaderMethods, \
                                HourlyGridFileManagerMethods
from atmosci.hourly.builder import HourlyGridBuilderMethods


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class NdfdGridFileReader(HourlyGridFileReaderMethods,
                             Hdf5HourlyGridFileReader):

    def __init__(self, hdf5_filepath):
        self._preInitHourlyFileReader_()
        Hdf5HourlyGridFileReader.__init__(self, hdf5_filepath)

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _loadManagerAttributes_(self):
        Hdf5HourlyGridFileReader._loadManagerAttributes_(self)
        self._loadHourGridAttributes_()


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class NdfdGridFileManagerMethods(HourlyGridFileManagerMethods):

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def inputProcessor(self, dataset_path):
        if '.' in dataset_path: key = dataset_path.split('.')[-1]
        else: key = dataset_path
        return self.processors.get(key.upper(), None)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def insertFudgedData(self, dataset_path, start_time, data, **kwargs):
        time_index = self.indexForTime(dataset_path, start_time, **kwargs)
        num_hours = \
            self._insertTimeSlice(dataset_path, data, time_index, **kwargs)
        prov_path = kwargs.get('provenance_path', 'provenance')
        self.insertProvenance(prov_path, start_time, data, source='fudged',
                              **kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def setForecastTimes(self, dataset_path, start_time, end_time):
        time_attrs = self.validateDataTimes(dataset_path, start_time, end_time)
        if time_attrs['first_fcast_time'] is None:
            self.setTimeAttribute(dataset_path, 'first_fcast_time', start_time)
        last_valid_time = time_attrs['last_valid_time']
        if last_valid_time is None or end_time > last_valid_time:
            self.setTimeAttribute(dataset_path, 'last_valid_time', end_time)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def updateForecast(self, dataset_path, start_time, data, **kwargs):
        if len(data.shape) == 2:
            end_time = start_time
        else:
            frequency = self.datasetAttribute(dataset_path,'feequency',1)
            end_hour = (data.shape[0]-1) * frequency
            end_time = start_time + datetime.timedelta(hours=end_hour)
        self.validateDataTimes(dataset_path, start_time, end_time)

        processor = self.inputProcessor(dataset_path)
        if processor is not None: data = processor(data)

        # update data
        time_index = self.indexForTime(dataset_path, start_time, **kwargs)
        self._insertTimeSlice(dataset_path, data, time_index, **kwargs)
        self.setForecastTimes(dataset_path, start_time, end_time)

        # uodate provenance when present
        prov_path = kwargs.get('provenance_path', 'provenance')
        if self.hasDataset(prov_path):
            self.insertProvenance(prov_path, start_time, data, **kwargs)
            self.setForecastTimes(prov_path, start_time, end_time)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def validateDataTimes(self, dataset_path, data_start_time, data_end_time):
        file_start_time = self.timeAttribute(dataset_path, 'start_time')
        if data_start_time < file_start_time:
            errmsg = 'Data start time (%s) is earlier than file start time (%s).'
            raise ValueError, errmsg % (data_start_time.strftime('%Y%m%d:%H'),
                                        file_start_time.strftime('%Y%m%d:%H'))
        file_end_time = self.timeAttribute(dataset_path, 'end_time')
        if data_start_time > file_end_time:
            errmsg = 'Data start time (%s) is later than file end time (%s).'
            raise ValueError, errmsg % (data_start_time.strftime('%Y%m%d:%H'),
                                        file_end_time.strftime('%Y%m%d:%H'))
        if data_end_time > file_end_time:
            errmsg = 'Data end time (%s) is later than file end time (%s).'
            raise ValueError, errmsg % (data_end_time.strftime('%Y%m%d:%H'),
                                        file_end_time.strftime('%Y%m%d:%H'))

        time_attrs = { 'file_end_time':file_end_time,
                       'file_start_time':file_start_time }
        time_attrs['first_fcast_time'] = \
            self.timeAttribute(dataset_path, 'first_fcast_time', None)
        time_attrs['last_valid_time'] = \
            self.timeAttribute(dataset_path, 'last_valid_time', None)

        return time_attrs

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _initDataProcessors_(self, **kwargs):
        # special processing rules for datasets
        self.processors = ConfigObject('processors', None)
        # humidity data input processor
        def processRhum(data):
            rhum = N.around(data, 2)
            rhum[N.where(rhum > 100)] = 100.
            rhum[N.where(rhum < 0)] = 0.
            return rhum
        self.processors.RHUM = processRhum

        # temperature data input processor
        def processTemp(data):
            return N.around(data, 2)
        self.processors.DPT = processTemp
        self.processors.TMP = processTemp

        # precip data input processor
        def processPcpn(data):
            data[N.where(data < 0.01)] = 0.
            return N.around(data, 2)
        self.processors.PCPN = processPcpn

        processors = kwargs.get('processors', None)
        if processors is not None:
            if isinstance(processors, dict):
                for key, function in processors.items():
                    self.processors[key] = function
            elif isinstance(processors, (tuple, dict)):
                for key, function in processors:
                    self.processors[key] = function
            else:
                errmsg = '"processors" kwarg must be dict, tuple or list : NOT '
                TypeError, errmsg + str(type(processors))


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class NdfdGridFileManager(NdfdGridFileManagerMethods,
                              Hdf5HourlyGridFileManager):

    def __init__(self, hdf5_filepath, mode='r', **kwargs):
        self._preInitHourlyFileManager_()
        Hdf5HourlyGridFileManager.__init__(self, hdf5_filepath, mode)
        self._initDataProcessors_(**kwargs)

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _loadManagerAttributes_(self):
        Hdf5HourlyGridFileManager._loadManagerAttributes_(self)
        self._loadHourGridAttributes_()
        self._loadProvenanceGenerators_()


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class NdfdGridFileBuilder(HourlyGridBuilderMethods, NdfdGridFileManager):
    """ Creates a new HDF5 file with read/write access to hourly 3D
        gridded data.
    """
    def __init__(self, hdf5_filepath, config, filetype, region, start_time,
                       end_time, timezone, lons=None, lats=None, **kwargs):
        if 'source' in kwargs:
            source = kwargs['source']
            del kwargs['source']
        else: source = config.sources.ndfd.grid
        print '\nNdfdGridFileBuilder.__init__ :', source, region, '\n'

        HourlyGridBuilderMethods.preInitBuilder(self, config, filetype,
                         region, source, timezone, start_time=start_time,
                         end_time=end_time, **kwargs)

        mode = kwargs.get('mode', 'w')
        if mode == 'w':
            self.load_manager_attrs = False
        else: self.load_manager_attrs = True

        NdfdGridFileManager.__init__(self, hdf5_filepath, mode)
        # set the time span for this file
        #self.initTimeAttributes(**kwargs)
        # close the file to make sure attributes are saved
        self.close()
        # reopen the file in append mode
        self.open(mode='a')
        # build lat/lon datasets if they were passed
        if lons is not None:
            self.initLonLatData(lons, lats, **kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def sourceFileAttributes(self, **kwargs):
        """
        resolve data source attributes for the file
        """ 
        source = self.source
        print '\n\nsourceFileAttributes :\n', source, '\n\n'
        region = kwargs.get('region', source.region)
        attrs = { 'bbox':kwargs.get('bbox', source.bbox[region]),
                  'description':kwargs.get('description', source.description),
                  'grid_type': source.grid_type,
                  'node_spacing': source.node_spacing,
                  'region': region,
                  'search_radius': source.search_radius,
                  'source': self.filetype.get('source', source.tag),
                  'timezone':source.file_timezone,
                }

        node_spacing = source.get('node_spacing',None)
        if node_spacing is not None:
            attrs['node_spacing'] = node_spacing 
        search_radius = source.get('search_radius',None)
        if search_radius is not None:
            attrs['search_radius'] = search_radius

        return attrs

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _fileDescription(self, source, **kwargs):
        return kwargs.get('source_description',
                   self.filetype.get('description', source.description))


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _resolveSourceAttributes(self, **kwargs): 
        """
        resolve data source attributes for a dataset
        """
        source = self.source
        attrs = {
                  'grid_type': kwargs.get('grid_type', source.grid_type),
                  'node_spacing': source.node_spacing,
                  'region': kwargs.get('region', source.region),
                  'resolution': kwargs.get('resolution', source.resolution),
                  'node_search_radius': source.search_radius,
                  'source': source.tag,
                }
        return attrs

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def _loadManagerAttributes_(self):
        if self.load_manager_attrs:
            NdfdGridFileManager._loadManagerAttributes_(self)
        else:
            self._loadProvenanceGenerators_()
            self.time_attr_cache = { }

