
import datetime
import warnings

import numpy as N

from atmosci.utils import tzutils
from atmosci.utils.timeutils import lastDayOfMonth

from atmosci.reanalysis.factory import NdfdGridFileFactory


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class SmartNdfdDataMethods:

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def slices(self, slice_start_time, slice_end_time):

        slices = [ ]
        start_time = tzutils.tzaDatetime(slice_start_time, self.tzinfo)
        end_time = tzutils.tzaDatetime(slice_end_time, self.tzinfo)

        if start_time.month == end_time.month:
            slices.append((start_time, end_time))
        else:
            last_day = lastDayOfMonth(start_time)
            month_end = start_time.replace(day=last_day, hour=23)
            slices.append((start_time, month_end))
            
            month = start_time.month + 1
            while month < end_time.month:
                month_start = start_time.replace(month=month, day=1, hour=0)
                last_day = lastDayOfMonth(month_start)
                month_end = month_start.replace(day=last_day, hour=23)
                slices.append((month_start, month_end))
            
            slices.append((end_time.replace(day=1, hour=0), end_time))

        return tuple(slices)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def timeSlice(self, variable, slice_start_time, slice_end_time, **kwargs):
        region = kwargs.get('region', self.region)

        slices = self.slices(slice_start_time, slice_end_time)
        num_hours = tzutils.hoursInTimespan(slices[0][0], slices[-1][1])

        # filter annoying numpy warnings
        warnings.filterwarnings('ignore',"All-NaN axis encountered")
        warnings.filterwarnings('ignore',"All-NaN slice encountered")
        warnings.filterwarnings('ignore',"invalid value encountered in greater")
        warnings.filterwarnings('ignore',"invalid value encountered in less")
        warnings.filterwarnings('ignore',"Mean of empty slice")
        # MUST ALSO TURN OFF WARNING FILTERS AT END OF SCRIPT !!!!!

        data = N.empty((num_hours,)+self.grid_dimensions, dtype=float)
        data.fill(N.nan)

        units = None
        prev_indx = 0
        for first_hour, last_hour in slices:
            sindx = prev_indx
            eindx = sindx + tzutils.hoursInTimespan(first_hour, last_hour)
            reader = \
                self.ndfdGridFileReader(first_hour, variable, region, **kwargs)
            data[sindx:eindx,:,:] = \
                reader.timeSlice(variable, first_hour, last_hour, **kwargs)
            if sindx == 0:
                units = reader.datasetAttribute(variable, 'units')
            prev_indx = eindx

        # turn annoying numpy warnings back on
        warnings.resetwarnings()

        if kwargs.get('lonlat', False):
            lats = reader.lats
            lons = reader.lons
            reader.close()
            return lons, lats, units, data
        else:
            reader.close()
            return units, data


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class SmartNdfdGridReader(SmartNdfdDataMethods, NdfdGridFileFactory):

    def __init__(self, region, **kwargs):
        ReanalysisGridFileFactory.__init__(self, **kwargs)
        self.region = region
        dims = self.source.dimensions[region]
        self.grid_dimensions = (dims.lat, dims.lon)

