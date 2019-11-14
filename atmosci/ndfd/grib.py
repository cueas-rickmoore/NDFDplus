
import os, sys
import warnings

import datetime
from dateutil.relativedelta import relativedelta

import numpy as N
import pygrib

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from atmosci.ndfd.config import CONFIG


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class NdfdGribNodeFinder(object):
    def __init__(self, grib_lons, grib_lats):
        self.grib_lons = grib_lons
        self.grib_lats = grib_lats

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def indexOfNearestNode(self, target_lon, target_lat, tolerance=None):
        # "closeness" is dependent on the projection and grid spacing of
        # the data ... this implementation is decent for grids in the
        # continental U.S. with small node spacing (~ 5km or less) such
        # as the ACIS 5 km Lambert Conformal grids supplied by NRCC.
        # It should really be implemented in a subclass using a method
        # that is specific to the grid type in use.

        # try for anexact fit
        indexes = N.where( (self.grib_lons == target_lon) &
                           (self.grib_lats == target_lat) )
        if len(indexes[0]) > 0:
            return indexes[0][0], indexes[1][0]

        # try to find a unique node within the tolerance
        if tolerance is not None:
            indexes = N.where( (self.grib_lons >= (target_lon - tolerance)) &
                               (self.grib_lons <= (target_lon + tolerance)) )
            unique_lons = N.unique(self.grib_lons[indexes])

            indexes = N.where( (self.grib_lats >= (target_lat - tolerance)) &
                               (self.grib_lats <= (target_lat + tolerance)) )
            unique_lats = N.unique(self.grib_lats[indexes])

            if len(unique_lons) == 1 and len(unique_lats) == 1:
                indexes = N.where( (self.grib_lons == unique_lons[0]) &
                                   (self.grib_lats == unique_lats[0]) )
                return indexes[0][0], indexes[1][0]

            # no unique node within tolerance, find any within tolerance 
            indexes = N.where( (self.grib_lons >= (target_lon - tolerance)) &
                               (self.grib_lons <= (target_lon + tolerance)) &
                               (self.grib_lats >= (target_lat - tolerance)) &
                               (self.grib_lats <= (target_lat + tolerance)) )
            if len(indexes[0]) == 1:
                return indexes[0][0], indexes[1][0]

            if len(indexes[0]) > 1:
                distances = self.distanceBetweenNodes(target_lon, target_lat,
                                                      self.grib_lons[indexes],
                                                      self.grib_lats[indexes])
                closest = N.where(distances == distances.min())
                return self.grib_lons[closest], self.grib_lats[closest]

        return None, None 

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def distanceBetweenNodes(self, target_lon, target_lat,
                                   lon_or_lons, lat_or_lats):
        lon_diffs = lon_or_lons - target_lon 
        lat_diffs = lat_or_lats - target_lat
        return N.sqrt( (lon_diffs * lon_diffs) + (lat_diffs * lat_diffs) )


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class NdfdGribFileIterator(object):
    """
    An iterator to sequentially return messages from  a grib file
    """

    def __init__(self, grib_filepath):
        self.gribs = pygrib.open(grib_filepath)
        self.messages = self.gribs.select()
        self.next_message = 0
        self.num_messages = len(self.messages)

    def __iter__(self):
        return self

    def close(self):
        self.gribs == None

    @property
    def first_message(self):
        return self.messages[0]

    @property
    def last_message(self):
        return self.messages[-1]

    def next(self):
        if self.next_message < self.num_messages:
            index = self.next_message
            self.next_message += 1
            return index, self.messages[index]

        self.gribs.close()
        self.messages == [ ]
        raise StopIteration


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class NdfdGribFileReader(object):
    """
    VERY SIMPLISTIC NDFD grib file reader
    """

    def __init__(self, grib_filepath, **kwargs):
        self.debug = kwargs.get('debug',False)
        self.verbose = kwargs.get('verbose',debug)
        self.__config = kwargs.get('config', CONFIG)
        self.__grib_filepath = grib_filepath
        self.__gribs = None
        self.__ndfd_config = ndfd = config.sources.ndfd
        self.__grib_source = ndfd[kwargs.get('source', ndfd.default_source)]
        self.variable_map = self.grib_source.variable_maps[timespan] 

    @property
    def conifg(self):
        return self.__config

    @property
    def gribs(self):
        return self.__gribs

    @property
    def filepath(self):
        return self.__grib_filepath

    @property
    def ndfd_config(self):
        return self.__ndfd_config

    @property
    def grib_source(self):
        return self.__grib_source

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def close(self):
        pygrib.close(self.__gribs)
        self.__gribs == None

    def open(self):
        self.__gribs = pygrib.open(self.__grib_filepath)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def explore(self):
        info = [ ]
        for index, grib in enumerate(self.gribs.select()):
            info.append( (index, grib.name, grib.shortName, grib.forecastTime,
                          grib.validDate) )
        return info

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def exploreInDetail(self):
        info = [ ]
        for index, grib in enumerate(self.gribs.select()):
            info.append( (index, grib.name, grib.shortName, grib.forecastTime,
                          grib.validDate, grib.dataDate, grib.dataTime,
                          grib.missingValue, grib.units, grib.values.shape) )
        return info

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def extractDailySlices(self, grib_key, time_span, indexes, shape,
                                 start_date=None):
        data_slices = [ ]
        #if temp_var == 'mint': daily = [(last_obs_date, last_obs_mint), ]
        #else: daily = [ ]
        if verbose: print '\nextracting forecast for', grib_key

        grib_var_name = self.__ndfd_config.variables[grib_key]
        grib = self.__gribs.select(name=grib_var_name)

        for message_num in range(len(grib)):
            message = grib[message_num]
            if self.debug: print message
            analysis_date = message.analDate
            if self.debug: print '    "analDate" =', analysis_date
            fcast_time = message.forecastTime
            if self.debug: print '        "forecast time" =', fcast_time
            if fcast_time > 158: # forecast time is MINUTES
                fcast_time = analysis_date + relativedelta(minutes=fcast_time)
            else: # forecast time is hours
                fcast_time = analysis_date + relativedelta(hours=fcast_time)
            if self.verbose: print '        forecast datetime =', fcast_time
            fcast_date = fcast_time.date()
            
            if start_date is None or fcast_date >= start_date:
                data = message.values[indexes].data
                data = data.reshape(shape)
                data[N.where(data == 9999)] = N.nan
                data = convertUnits(data, 'K', 'F')
                data_slices.append((fcast_date, data))
            else:
                if self.verbose: print '        ignoring fcast for', fcast_date

        return data_slices

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # pygrib : message attribute access 
    #
    #  grib.attr_name i.e. _getattr_(attr_name) # returns attribute value
    #                  _getattribute_(attr_name) # returns attribute value
    #  grib[key] i.e. _getitem_(key) # returns value associated with grib key
    #
    # pygrib : message functions
    #
    #   data(lat1=None,lat2=None,lon1=None,Lon2=None)
    #        # returns data, lats and lons for the bounding box
    #   has_key(key) # T/F whether grib has the specified key
    #   is_missing(key) # True if key is invalid or value is equal to
    #                   # the missing value for the message
    #   keys() # like Python dict keys function
    #   latlons() # return lats/lons as NumPy array
    #   str(grib) or repr(grib)
    #                i.e. repr(grib) # prints inventory of grib
    #   valid_key(key) # True only if the grib message has a specified key,
    #                  # it is not missing and it has a value that can be read
    #
    # pygrib : message instance variables
    #    analDate     !!! often "unknown" by pygrib
    #    validDate ... value is datetime.datetime
    #    fcstimeunits ... string ... usually "hrs"
    #    messagenumber ... int ... index of grib in file
    #    projparams ... proj4 representation of projection spec
    #    values ... return data values as a NumPy array
    #
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

