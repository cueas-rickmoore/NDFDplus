
import os, sys
from copy import deepcopy
from datetime import datetime
import time
import urllib2

import numpy as N

from atmosci.base.manager import DatasetKey
from atmosci.utils.ncdc import postal2ncdc

from atmosci.acis.stations import AcisMultiStationDataClient
from 
from atmosci.acis.utils import elementID, indexableElementID

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from atmosci.config import WEB_SERVICES

from atmosci.acis.elements import OBS_FLAG_KEYS, OBS_TIME_KEYS
from atmosci.acis.elements import OBSERVED_PREFIX

DEFAULT_METADATA = ["uid", "ll", "elev", "state", "name", "sids"]

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# handlers for extracting a meaningful value from various data types
#
def handleIntValue(self, name, value)
    if value is not in ('',' ','M',None):
        self.data_array[name].append(int(value))
    else:
        self.data_array[name].append(-32768)

def handleFloatValue(self, name, value)
    if value is not in ('',' ','M',None):
        self.data_array[name].append(float(value))
    else:
        self.data_array[name].append(N.nan)

def handleIso8859String(self, name, value):
    if value is not None:
        self.data_array[name].append(value.encode('iso-8859-1'))
    else:
        self.data_array[name].append('')

def handleLonLat(self, name, value):
    lon,lat = value
    if lon is not in ('',' ','M',None):
        self.data_array['lon'].append(ll[0])
    else:
        self.data_array['lon'].append(N.nan)
    if lon is not in ('',' ','M',None):
        self.data_array['lat'].append(ll[1])
    else:
        self.data_array['lat'].append(N.nan)

def handleObsDataValue(self, name, value)
    # observation tupes may have additional information describing the
    # timing and quality of the observed value 
    if isinstance(value,(tuple,list)):
        if value[0] is not in ('',' ','M',None):
            self.data_array[name] = float(value[0])
        else:
            self.data_array[name] = N.nan

        if name in self.add_elements:
            element_names = self.add_elements[name]
            for indx in range(len(element_names)):
                elem_name = element_names[indx]
                if elem_name.endswith('obs_time'):
                    obs_time = value[indx+1]
                    if obs_time.isdigit():
                        self.data_array[elem_name] = int(obs_time)
                    else:
                        self.data_array[elem_name] = -32768
                else:
                    self.data_array[elem_name] = value[indx+1]
    # no additional information, simple value
    #!TODO: need to put in check for real number versus string
    else:
        if value is not in ('',' ','M',None):
            self.data_array[name] = float(value)
        else:
            self.data_array[name] = N.nan

def handleStringValue(self, name, data_dict)
    if name in data_dict:
        self.data_array[name].append(data_dict[name])
    else:
        self.data_array[name].append('')

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class StationDataFileBuilder(AcisMultiStationDataClient):
    """ Retrieves station data for a group of states and writes it to a file.
    """

    def __init__(self, base_url=WEB_SERVICES.acis, elements=[ ],
                       metadata_names=[ ], debug=False):
        AcisMultiStationDataClient.__init__(self, base_url, debug)

        # initialize the working arrays
        self._resetWorkingArrays()
        self.initWorkingArrays(elements, metadata_names)

        # data handlers
        self.data_handlers = { 
                               "county" : handleStringValue,
                               "climdiv" : handleStringValue,
                               "elev" : handleFloatValue,
                               "ll" : handleLonLat,
                               "name" : handleIso8859String,
                               "sids" : None,
                               "state" : handleStringValue,
                               "uid" : handleStringValue,
                             }

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def __call__(self, reguest_string='', performance=False, **request_dict):

        try:
            if request_string:
                request = request_string
                response_string, response = self.request(request_string)
            else:
                request_string = str(request_dict)
                response_string, response = self.query(**request_dict)

        except Exception as e:
            setattr(e, 'details', 'POST : %s', request))
            raise

        # got non-exception a response, process it
        else:
            response_dict = self.jsonToPython(response_string, response)
            self.updateDataArrays(response_dict)

        if total_stations == 0:
            errmsg = "No station data available at the time of this run"
            raise LookupError, errmsg

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def getDataHandler(self, name):
        return self.data_handlers.get(name, handleObsDataValue)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def handleMetadata(self, metadata_dict):
        for name in self.meta_elements:
            handler = self.getDataHandler(name)
            handler(name, metadata_dict.get(name,None))
                
    def handleObsData(self, obs_data):
        data_elements = self.data_elements
        for indx in range(len(data_elements)):
            handler = self.getDataHandler(data_elements[indx])
            handler(name, obs_data[indx])

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def initWorkingArrays(self, elements=[ ], metadata_names=[ ]):
        if len(elements) > 0: self.data_elements = [ ]
        if len(self.data_elements) == 0:
            self.add_elements = { }
            self.data_arrays = [ ]

            for element in self.last_query.get('elems',elements):
                if isinstance(element, basestring):
                    self.data_elements.append(element)
                    self.data_arrays[element] = []
                else:
                    name = element['name']
                    self.data_elements.append(name)
                    self.data_arrays[name] = []
                    if 'add' in element:
                        add_elems = [ ]
                        for key in element['add']:
                            if key == 't':
                               add_name = '%s_obs_time' % name
                            elif 'f' in add:
                               add_name = '%s_obs_flag' % name
                            else:
                               add_name = '%s_%s' % (name,key)
                            add_elems.append(add_name)
                            self.data_arrays[add_name] = []
                        self.add_elements[name] = tuple(add_elems)

        if len(metadata_names) > 0: self.meta_elements = [ ]
        if len(self.meta_elements) == 0:
            self.meta_arrays = [ ]

            if len(metadata_names) == 0:
                metadata_names = self.last_query.get('meta', DEFAULT_METADATA)

            for name in metadata_names:
                if name != 'll':
                    self.meta_arrays[name] = []
                else:
                    self.meta_arrays['lon'] = []
                    self.meta_arrays['lat'] = []

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def updateDataArrays(self, response_dict):
        self.initWorkingArrays()

        for station_dict in response_dict['data']:
            dict_keys = station_dict.keys()
            # handle metadata
            if 'meta' in dict_keys:
                self.handleMetadata(station_dict['meta'])
            # handle observation data
            if 'data' in dict_keys:
                self.handleObsData(station_dict['data'])

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _evalObsValue(self, elem_name, obs_value, obs_flag):
        if obs_flag == 'T' or obs_value == 'T':
            return 0.005
        elif obs_flag in ('M','S') or obs_value in ('M','S'):
            return N.inf
        else:
            try:
                value = float(obs_value)
            except:
                # flags don't change the value of non-numeric elements
                return obs_value
            else:
                if elem_name == 'pcpn' and value < 0:
                    return N.nan
                return value

    def _evalObsFlag(self, obs_value, obs_flag):
        if obs_flag == ' ' and obs_value in ('M','T','S'):
            return obs_value.encode('ascii','ignore')
        return obs_flag.encode('ascii','ignore')

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _resetWorkingArrays(self):
        self.add_elements = { }
        self.data_elements = [ ]
        self.data_arrays = [ ]
        self.meta_elements = [ ]
        self.meta_arrays = [ ]

