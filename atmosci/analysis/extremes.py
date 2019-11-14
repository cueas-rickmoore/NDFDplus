
import os
from datetime import datetime

import numpy as N
from scipy.interpolate import UnivariateSpline

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def interpolateExtremes(time_series_array, points_per_knot=5, smoothing=0,
                        missing_value=None, debug=False):
    """ Uses a univariate spline to interpolate a minimum and maximum
    value and the time interval at which they occurred.

    NOTE: The first and last nodes in the time series array will be
          ignored when looking for the min/max values. These nodes
          should be populated with data in the normal flow of the
          time series but outside the time sequence of interest.

    WARNING: When there are missing values in the time series array,
             the spline cannot be applied and the simple min/max values
             are returned.

    Returns: min_value, min_index, max_value, max_index
        min_value - minimum value discovered
        min_index - index into time series array for min value
        max_value - maximum value discovered
        max_index - index into time series array for max value

    NOTE: In all cases, the intdex reported is the first occurrence of
          the min/max value in the time series array.
    """
    if time_series_array.dtype.kind != 'f':
        raise ValueError, 'Interpolation can only be done on float arrays'

    # look for missing or invalid values
    valid_data = time_series_array[N.where(N.isfinite(time_series_array))]
    if missing_value is not None:
        valid_data = valid_data[N.where(valid_data != missing_value)]

    # all data in the time series array is valid
    if len(valid_data) == len(time_series_array):
        # determine the data knot intervals and calculate the spline
        num_intervals = len(time_series_array)
        data_knot_intervals = range(num_intervals)
        if debug:
            print 'total intervals available', num_intervals
            print 'data knots at', data_knot_intervals
        spline = UnivariateSpline(data_knot_intervals, time_series_array,
                                  s=smoothing)

        # determine the intermediate knot locations the apply the spline
        num_interp_intervals = (data_knot_intervals[-1] * points_per_knot) + 1
        interp_intervals = range(num_interp_intervals)
        interp_data_points = tuple([indx*0.2 for indx in interp_intervals])
        if debug:
            print 'interpolated points per data knot', points_per_knot
            print 'interpolate data at', interp_data_points

        interp_data_values = spline(interp_data_points)
        # don't want to include first and last nodes or their interp points
        first = points_per_knot
        last = -points_per_knot
        # at this point, it is easier to deal with the data as a python tuple
        usable_values = tuple(interp_data_values[first:last])
        max_value = max(usable_values)
        # index of time series node at or below max value 
        max_index = usable_values.index(max_value) + 1
        # for max, we always want index of node at or above max value 
        if max_index != ((max_index / points_per_knot) * points_per_knot):
            max_indx += 1
        min_value = min(usable_values)
        # index of time series node at or below min value ... DO NOT ADJUST
        min_index = usable_values.index(min_value) + 1

    # there is missing or invalid data in the time series array
    else:
        # trim first/last nodes - they are only used for interpolation 
        usable_data = time_series_array[1:-1]
        # trim any nodes with invalid data values
        valid_data = usable_data[N.where(N.isfinite(usable_data))]
        # trim any nodes with missing values
        if missing_value is not None:
            valid_data = valid_data[N.where(valid_data != missing_value)]

        # at this point, it is easier to deal with the data as a python tuple
        valid_data = tuple(valid_data)
        max_value = max(valid_data)
        min_value = min(valid_data)
        usable_data = tuple(usable_data)
        max_index = usable_data.index(max_value) + 1
        min_index = usable_data.index(min_value) + 1

    return min_value, min_index, max_value, max_index

