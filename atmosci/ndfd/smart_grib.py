
import os
import datetime
ONE_HOUR = datetime.timedelta(hours=1)

import numpy as N
import pygrib

from atmosci.utils.tzutils import asUTCTime

from atmosci.ndfd.factory import NdfdGribFileFactory


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from atmosci.ndfd.config import CONFIG
VALID_TIMESPANS = tuple(CONFIG.sources.ndfd.variables.keys())

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

BAD_FILL_METHOD = '"%s" fill method is not supported. Must be one of\n'
BAD_FILL_METHOD += '"avg", "constant", "scale" or "spread".'
BAD_TIMESPAN = '"%s" is not a valid timespan. Must be one of '
BAD_TIMESPAN += ','.join(['"%s"' % span for span in VALID_TIMESPANS])

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def hoursInTimespan(time1, time2, inclusive=True):
    if time1 > time2: diff = (time1 - time2)
    else: diff = (time2 - time1)
    if inclusive: return (diff.days * 24) + (diff.seconds/3600) + 1
    else: return (diff.days * 24) + (diff.seconds/3600)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def reshapeGrid(grib_msg, missing_value, grib_indexes, grid_shape_2D,
                grid_mask, decimals=2):
    values = grib_msg.values[grib_indexes].reshape(grid_shape_2D)
    if N.ma.is_masked(values): values = values.data
    values[N.where(values >= missing_value)] = N.nan
    values[N.where(grid_mask == True)] = N.nan
    return N.around(values,decimals)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class SmartNdfdGribFileReader(NdfdGribFileFactory):

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def completeInitialization(self, **kwargs):
        # initialize additonal factory/reader attributes  
        self.grib_region = kwargs.get('grib_region', self.ndfd.default_region)
        self.grib_source = \
             self.ndfd[kwargs.get('grib_source', self.ndfd.default_source)]
        self.gribs = None

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def dataForRegion(self, fcast_date, variable, timespans, grid_region,
                            grid_source, fill_gaps=False,
                               graceful_fail=False, debug=False):
        """
        Returns a sequence containing (relative hour, full forecast date,
        numpy array) tuples for each message in the grib files for each
        timezone in the list. Arrays are cleaned so that both masked and
        missing values are set to N.nan. The shape of each returned array
        is the same (num_lons x num_lats)

        Assumes file contains a range of times for a single variable.
        """
        data_records = [ ]

        if isinstance(timespans, basestring):
            timespans = (timespans,)
        elif not isinstance(timespans, (tuple, list)):
            errmsg = '"%s" is an invalid type for timespans argument.'
            errmsg += '\nArgument type must be one of string, list, tuple.'
            raise TypeError, errmsg % type(timespan)

        for timespan in timespans:
            if timespan not in VALID_TIMESPANS:
                raise ValueError, BAD_TIMESPAN % timespan

        # parameters for reshaping the grib arrays
        grid_shape_2D, grib_indexes, grid_mask = \
            self.gribToGridParameters(grid_source, grid_region)

        # code for filling gaps between records
        if fill_gaps:
            prev_record = None

            fill_method = \
                self.variableConfig(variable, timespan).fill_gaps_with

            for timespan in timespans:
                self.openGribfile(fcast_date, variable, timespan)
                # retrieve pointers to all messages in the file
                messages = self.gribs.select()
                first_msg = messages[0]
                missing = float(first_msg.missingValue)
                units = first_msg.units

                # fill the gap between this timespan and the previous one
                if not prev_record is None:
                    grid = reshapeGrid(first_msg, missing, grib_indexes,
                                       grid_shape_2D, grid_mask)
                    next_record = ('ndfd',asUTCTime(first_msg.validDate),grid)
                    data_records.extend(self.fillTimeGap(prev_record,
                                             next_record, fill_method))

                # update with records for the current timespan
                data = self.dataWithoutGaps(messages, fill_method, missing,
                            grib_indexes, grid_shape_2D, grid_mask, debug)
                data_records.extend(data)

                # track last record in previous timespan
                msg = messages[-1]
                grid = reshapeGrid(msg, missing, grib_indexes,
                                        grid_shape_2D, grid_mask)
                prev_record = ('ndfd', asUTCTime(msg.validDate), grid)

                self.closeGribfile()
        
        # code that preserves gaps between records
        else:

            for timespan in timespans:
                self.openGribfile(fcast_date, variable, timespan)
                # retrieve pointers to all messages in the file
                messages = self.gribs.select()
                first_msg = messages[0]
                missing = float(first_msg.missingValue)
                units = first_msg.units

                for msg in messages:
                    grid = reshapeGrid(msg, missing, grib_indexes,
                                       grid_shape_2D, grid_mask)
                    this_time =  asUTCTime(msg.validDate)
                    data_records.append(('ndfd', this_time, grid))
                    if debug:
                        stats = (N.nanmin(grid), N.nanmax(grid))
                        print 'value stats :', msg.validDate, stats
                self.closeGribfile()

        return units, data_records

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def dataWithoutGaps(self, messages, fill_method, missing, grib_indexes,
                              grid_shape_2D, grid_mask, debug=False):
        data_records = [ ]

        first_msg = messages[0]
        prev_grid = reshapeGrid(first_msg, missing, grib_indexes,
                                grid_shape_2D, grid_mask)
        prev_time = asUTCTime(first_msg.validDate)
        prev_record = ('ndfd', prev_time, prev_grid)
        if debug:
            stats = (N.nanmin(prev_grid), N.nanmax(prev_grid))
            print 'value range :', prev_time, stats

        open_gap = False
        for msg in messages[1:]:
            grid = reshapeGrid(msg, missing, grib_indexes, grid_shape_2D,
                               grid_mask)
            this_time =  asUTCTime(msg.validDate)
            this_record = ('ndfd', this_time, grid)
            if debug:
                stats = (N.nanmin(grid), N.nanmax(grid))
                print 'value stats :', msg.validDate, stats

            gap = hoursInTimespan(prev_time, this_time, inclusive=False)
            if gap > 1:
                records = \
                    self.fillTimeGap(prev_record, this_record, fill_method)
                # check whether previous record was replaced
                # if the list comprehension is empty, it needs to be added
                if not [rec for rec in records if rec[1] == prev_record[1]]:
                    data_records.append(prev_record)
                # add the gap records
                data_records.extend(records)
                # there is an open gap
                open_gap = True
            else: # no gap, add previous record
                data_records.append(prev_record)
                open_gap = False

            prev_record = this_record
            prev_time = this_time

        if not open_gap: # at this point prev_record == last this_record
            data_records.append(prev_record)

        if debug:
            msg = messages[-1]
            grid = reshapeGrid(msg, missing, grib_indexes, grid_shape_2D,
                               grid_mask)
            print '\nlast msg :', msg.validDate, N.nanmin(grid), N.nanmax(grid)

        return data_records

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def fillTimeGap(self, fcast1, fcast2, fill_method, decimals=2):
        # Assume both forecasts have the same source source
        base_source = fcast1[0]

        # Assume that the goal is to fill a time gap bewtween the
        # earlier time and the later time. So make sure we get the 
        # forecasts in the correct order.
        if fcast2 > fcast1:
            base_grid = fcast1[-1]
            base_time = fcast1[1]
            end_grid = fcast2[-1]
            end_time = fcast2[1]
        else: # just in case the rules are not followed
            base_grid = fcast2[-1]
            base_time = fcast2[1]
            end_grid = fcast1[-1]
            end_time = fcast1[1]

        # number of hours in the gap
        num_hours = hoursInTimespan(end_time, base_time, inclusive=False)

        gap = [ ]
        if num_hours > 1:
            # avg : all hours in the gap are filled by the average
            #       caclulated by dividing base time by number of hours
            # NOTE: the base fcast data is also replaced by the average
            if fill_method == 'avg':
                avg_grid = N.around(base_grid / num_hours, decimals)
                source = '%s* avg' % base_source
                gap.append( ('%s* avg' % base_source, base_time, avg_grid) )
                source = '%s avg' % base_source
                for n in range(1, num_hours):
                    gap.append( (source, base_time+datetime.timedelta(hours=n),
                                 avg_grid) )
            # constant : all hours in gap have same values as the base time 
            elif fill_method == 'constant':
                gap.append( (base_source, base_time, base_grid) )
                source = '%s constant' % base_source
                for n in range(1, num_hours):
                    gap.append( (source, base_time+datetime.timedelta(hours=n),
                                 base_grid) )
            # scale : increment each hour in the gap by the average
            #         difference b/w end time and base time data values
            elif fill_method == 'scale':
                gap.append( (base_source, base_time, base_grid) )
                avg_grid = N.around((end_grid-base_grid) / num_hours, decimals)
                source = '%s scaled' % base_source
                for n in range(1, num_hours):
                    gap.append( (source, base_time+datetime.timedelta(hours=n),
                                 base_grid + (avg_grid * n)) )
            # spread : give each time in the gap exactly the same fraction of
            #          the base_grid value
            # NOTE: the base fcast data is also replaced by the fraction
            elif fill_method == 'spread':
                fraction = N.around(base_grid / num_hours,decimals)
                gap.append( ('%s* spread' % base_source, base_time, fraction) )
                source = '%s spread' % base_source
                for n in range(1, num_hours):
                    gap.append( (source, base_time+datetime.timedelta(hours=n),
                                 fraction) )
            else:
                raise ValueError, BAD_FILL_METHOD % fill_method

        return gap

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def gribToGridParameters(self, grid_source, grid_region):
        reader = self.staticFileReader(grid_source, grid_region)
        grid_shape_2D, grib_indexes = reader.gribSourceIndexes('ndfd')
        grid_mask = reader.getData('cus_mask')
        reader.close()
        del reader
        return grid_shape_2D, grib_indexes, grid_mask

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def gridForRegion(self, fcast_date, variable, timespan, grid_region,
                            grid_source, fill_gaps=False, graceful_fail=False,
                            debug=False):
        """
        Returns a 3D NumPy grid containing data at all nodes in the grid
        region for all messages in file.
        
        Shape of returned grid is [num_hours, num_lons, num_lats]

        Assumes file contains a range of time periods for a single variable.
        """
        self.openGribfile(fcast_date, variable, timespan)
        # retrieve pointers to all messages in the file
        messages = self.gribs.select()

        first_msg = messages[0]
        first_hour = first_msg.validDate
        missing = float(first_msg.missingValue)
        units = first_msg.units

        if debug:
            print '\nfirst message :'
            print '    anal date :', first_msg.analDate
            print 'forecast hour :', first_msg.forecastTime
            print 'forecast date :', first_hour
            print 'missing value :', missing
            print '        units :', units
            print '\n 2nd message :'
            print '    anal date :', messages[1].analDate
            print 'forecast hour :', messages[1].forecastTime
            print 'forecast date :', messages[1].validDate
            print '\nlast message :', len(messages)
            print '    anal date :', messages[-1].analDate
            print 'forecast hour :', messages[-1].forecastTime
            print 'forecast date :', messages[-1].validDate
         
        num_hours = hoursInTimespan(messages[-1].validDate, first_hour, True)
        if debug:
            print '    time span :', num_hours
            print '\n'

        # parameters for reshaping the grib arrays
        grid_shape_2D, grib_indexes, grid_mask = \
            self.gribToGridParameters(grid_source, grid_region)

        grid = N.empty((num_hours,)+grid_shape_2D, dtype=float)
        grid.fill(N.nan)

        times = [ ]
        prev_time = first_hour
        prev_index = None
        for msg in messages:
            values = reshapeGrid(msg, missing_value, grib_indexes,
                                 grid_shape_2D, grid_mask)
            next_time = msg.validDate
            next_index = hoursInTimespan(next_time, first_hour, inclusive=False)
            grid[next_index,:,:] = values
            next_record = ('ndfd', next_time, values)

            if fill_gaps and prev_index:
                fill = self.variableConfig(variable, timespan).fill_gaps_with
                gap_info = self.fillTimeGap(prev_record, next_record, fill)
                for src, fcast_time, values in gap_info:
                    index = hoursInTimespan(fcast_time, prev_time, False)
                    grid[index,:,:] = values
                    if debug: print '     gap date :', index, fcast_time
            if debug: print 'forecast date :', next_index, next_time

            prev_record = next_record
            prev_index = next_index

        gribs.close()

        return asUTCTime(first_hour), units, grid

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def closeGribfile(self):
        self.gribs.close()
        self.gribs = None

    def openGribfile(self, fcast_date, variable, timespan, **kwargs):
        if self.gribs != None: self.closeGribfile()
        grib_filepath = \
            self.ndfdGribFilepath(fcast_date, variable, timespan, **kwargs)
        self.gribs = pygrib.open(grib_filepath)

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

