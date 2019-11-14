
import numpy as N


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# individual functions
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def accumulateGDD(daily_gdd, axis=0):
    """ Calculate accumulated GDD from a NumPy array of daily GDD values.

    Arguments
    --------------------------------------------------------------------
    daily_gdd  : NumPy float array of daily GDD
    axis       : axis along which GDD is to be calculated.
                 NOTE: not used for 1D arrray and defaults to 0 for
                       multi-dimension arrays. 

    Returns
    --------------------------------------------------------------------
    NumPy array af the same dimensions as the input gdd array.
    """
    if daily_gdd.ndim > 1: return N.cumsum(daily_gdd, axis=axis)
    else: return N.cumsum(daily_gdd, axis=None)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def calcAvgGDD(gdd_array, axis=0):
    """ Calculate the average for a NumPy array of GDD values.

    Arguments
    --------------------------------------------------------------------
    gdd_array  : NumPy float array
    axis       : axis along which average GDD is to be calculated.
                 NOTE: not used for 1D arrray and defaults to 0 for
                       multi-dimension arrays. 

    Returns
    --------------------------------------------------------------------
    float value for 1D input
    OR
    NumPy array with 1 fewer dimensions than the input gdd array.
    """
    if isinstance(gdd_array, N.ndarray):
        if gdd_array.ndim > 1:
            return N.round(N.nanmean(gdd_array, axis=axis) + .01)
        else: return N.round(N.nanmean(gdd_array) + .01)
    else:
        raise TypeError, 'gdd argument must be a NumPy array'

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def calcAvgTemp(maxt, mint):
    """ Calculate the average temperature 

    Arguments
    --------------------------------------------------------------------
    maxt : maximum temperature
    mint : minimum temperature

    NOTE: maxt and mint arguments may be:
          1) NumPy arrays of floats or ints 
          2) single float or int
          3) list or tuple of floats or ints 
             NOTE: this is very inefficient if there are more than
                   a few values in the sequence

    Returns
    --------------------------------------------------------------------
    calculated average temperature of same type and size as input temps
    """
    if isinstance(mint, N.ndarray):
        return N.round(((maxt + mint) * 0.5) + .01)
    elif isinstance(mint, (int,float)):
        return round(((maxt + mint) * 0.5) + .01)
    elif isinstance(mint, (list,tuple)):
        avgt = [ N.round(((maxt[indx] + mint[indx]) * 0.5) + .01)
                 for indx in range(len(mint)) ]
        if isinstance(mint, tuple): return tuple(avgt) 
        return avgt 
    else: # unsupported type
        errmsg = '%s is an unsupported type for temperature arguments'
        raise TypeError, errmsg % type(mint)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def calcGDD(avgt, threshold):
    """ Calculate the GDD using average temperature based on commonly
    accepted GDD threshold rules.

    Arguments
    --------------------------------------------------------------------
    avgt      : average temperature
    threshold : GDD threshold specification. Pass a single int for 
                caclutations using only a low temperature threshold.
                Pass a tuple/list for calculations using both upper and
                lower GDD thresholds.

    NOTE: avgt argument may be float, int or a NumPy array of floats.

    Returns
    --------------------------------------------------------------------
    GDD calculated average temperature of same type and size as avgt
    """
    # extract lo/hi thresholds
    if isinstance(threshold, (list,tuple)):
        if threshold[0] > threshold[1]:
            hi_thold, lo_thold = threshold
        else: lo_thold, hi_thold = threshold
    else:
        lo_thold = threshold
        hi_thold = None

    if isinstance(avgt, N.ndarray):
        # create a zero gdd array ... calculated GDD will be added to it
        gdd = N.zeros_like(avgt)
        # use only avg temps >= low threhsold
        indexes = N.where(avgt >= lo_thold)
        if len(indexes[0]) > 0: 
            # construct temporary array only where avgt >= low threshold
            _avgt = avgt[indexes]
            if hi_thold: # when specified, use only avg temps <= hi threshold
                _avgt[N.where(_avgt > hi_thold)] = hi_thold
            # only calculate GDD using adjusted avg temps
            gdd[indexes] = _avgt - lo_thold
        # set nodes where avgt is NAN to NAN
        gdd[N.where(N.isnan(avgt))] = N.nan
        return gdd

    # single value
    elif isinstance(avgt, (int, float)):
        # use only avg temps >= low threhsold
        if avgt >= lo_thold:
            # when specified, use only avg temps <= hi threshold
            if hi_thold:
                if avgt > hi_thold: return hi_told - lo_thold
            return avgt - lo_thold
        else: return type(avgt)(0)

    else: # unsupported type
        errmsg = '%s is an unsupported type for "avgt" argument'
        raise TypeError, errmsg % type(avgt)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def gddThresholdAsString(gdd_threshold):
    if isinstance(gdd_threshold, (list,tuple)):
        return ''.join(['%02d' % th for th in gdd_threshold])
    elif isinstance(gdd_threshold, int):
        return '%02d' % gdd_threshold
    elif isinstance(gdd_threshold, basestring):
        return gdd_threshold
    else: return str(gdd_threshold)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def roundGDD(data):
    """ round all values in data array using GDD rounding criteria
    """
    if isinstance(data, (N.ndarray, int, float)):
        return N.round(data + .01)

    else: # unsupported type
        errmsg = '%s is an unsupported type for "data" argument'
        raise TypeError, errmsg % type(data)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# class wrappers for individual function
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class GDDCalculatorMethods:

    def accumulateGDD(self, daily_gdd, axis=None):
        """ Calculate accumulated GDD from a NumPy array of daily GDD values.

        Arguments
        --------------------------------------------------------------------
        daily_gdd  : NumPy float array of daily GDD
        axis       : axis along which GDD is to be calculated.
                     NOTE: not used for 1D arrray and defaults to 0 for
                           multi-dimension arrays. 

        Returns
        --------------------------------------------------------------------
        NumPy array af the same dimensions as the input gdd array.
        """
        if daily_gdd.ndim > 1: return N.cumsum(daily_gdd, axis=axis)
        else: return N.cumsum(daily_gdd, axis=None)

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def calcAvgGDD(self, gdd_array, axis=0):
        """ Calculate the average for a NumPy array of GDD values.

        Arguments
        --------------------------------------------------------------------
        gdd_array  : NumPy float array
        axis       : axis along which average GDD is to be calculated.
                     NOTE: not used for 1D arrray and defaults to 0 for
                           multi-dimension arrays. 

        Returns
        --------------------------------------------------------------------
        float value for 1D input
        OR
        NumPy array with 1 fewer dimensions than the input gdd array.
        """
        if isinstance(gdd_array, N.ndarray):
            if gdd_array.ndim > 1:
                return N.round(N.nanmean(gdd_array, axis=axis) + .01)
            else: return N.round(N.nanmean(gdd_array) + .01)
        else:
            raise TypeError, 'gdd argument must be a NumPy array'

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def calcAvgTemp(self, maxt, mint):
        """ Calculate the average temperature 

        Arguments
        --------------------------------------------------------------------
        maxt : maximum temperature
        mint : minimum temperature

        NOTE: maxt and mint arguments may be:
              1) NumPy arrays of floats or ints 
              2) single float or int
              3) list or tuple of floats or ints 
                 NOTE: this is very inefficient if there are more than
                       a few values in the sequence

        Returns
        --------------------------------------------------------------------
        calculated average temperature of same type and size as input temps
        """
        if isinstance(mint, N.ndarray):
            return N.round(((maxt + mint) * 0.5) + .01)
        elif isinstance(mint, (int,float)):
            return round(((maxt + mint) * 0.5) + .01)
        elif isinstance(mint, (list,tuple)):
            avgt = [ N.round(((maxt[indx] + mint[indx]) * 0.5) + .01)
                     for indx in range(len(mint)) ]
            if isinstance(mint, tuple): return tuple(avgt) 
            return avgt 
        else: # unsupported type
            errmsg = '%s is an unsupported type for temperature arguments'
            raise TypeError, errmsg % type(mint)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def calcGDD(self, avgt, threshold):
        """ Calculate the GDD using average temperature based on commonly
        accepted GDD threshold rules.

        Arguments
        --------------------------------------------------------------------
        avgt      : average temperature
        threshold : GDD threshold specification. Pass a single int for 
                    caclutations using only a low temperature threshold.
                    Pass a tuple/list for calculations using both upper and
                    lower GDD thresholds.

        NOTE: avgt argument may be float, int or a NumPy array of floats.

        Returns
        --------------------------------------------------------------------
        GDD calculated average temperature of same type and size as avgt
        """
        # extract lo/hi thresholds
        if isinstance(threshold, (list,tuple)):
            if threshold[0] > threshold[1]:
                hi_thold, lo_thold = threshold
            else: lo_thold, hi_thold = threshold
        else:
            lo_thold = threshold
            hi_thold = None

        if isinstance(avgt, N.ndarray):
            # create a zero gdd array ... calculated GDD will be added to it
            gdd = N.zeros_like(avgt)
            # use only avg temps >= low threhsold
            indexes = N.where(avgt >= lo_thold)
            if len(indexes[0]) > 0: 
                # construct temporary array only where avgt >= low threshold
                _avgt = avgt[indexes]
                if hi_thold: # when specified, use only avg temps <= hi threshold
                    _avgt[N.where(_avgt > hi_thold)] = hi_thold
                # only calculate GDD using adjusted avg temps
                gdd[indexes] = _avgt - lo_thold
            # set nodes where avgt is NAN to NAN
            gdd[N.where(N.isnan(avgt))] = N.nan
            return gdd

        # single value
        elif isinstance(avgt, (int, float)):
            # use only avg temps >= low threhsold
            if avgt >= lo_thold:
                # when specified, use only avg temps <= hi threshold
                if hi_thold:
                    if avgt > hi_thold: return hi_told - lo_thold
                return avgt - lo_thold
            else: return type(avgt)(0)

        else: # unsupported type
            errmsg = '%s is an unsupported type for "avgt" argument'
            raise TypeError, errmsg % type(avgt)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def gddThresholdAsString(self, gdd_threshold):
        if isinstance(gdd_threshold, (list,tuple)):
            return ''.join(['%02d' % th for th in gdd_threshold])
        elif isinstance(gdd_threshold, int):
            return '%02d' % gdd_threshold
        elif isinstance(gdd_threshold, basestring):
            return gdd_threshold
        else: return str(gdd_threshold)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def roundGDD(self, data):
        """ round data using GDD rounding criteria
        """
        if isinstance(data, (N.ndarray, int, float)):
            return N.round(data + .01)

        else: # unsupported type
            errmsg = '%s is an unsupported type for "data" argument'
            raise TypeError, errmsg % type(data)

GDDFunctionsMixin = GDDCalculatorMethods # backwards compatible


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class CallableGDDCalculator(GDDCalculatorMethods):

    def __call__(self, maxt, mint, gdd_threshold):
        """ Calculate the growoing degree days from max & min temperatures
        using on commonly accepted GDD threshold rules.

        Arguments
        --------------------------------------------------------------------
        maxt      : maximum temperature
        mint      : minimum temperature
        threshold : GDD threshold specification. Pass a single int for 
                    caculatations using only a low temperature threshold.
                    Pass a tuple/list for calculations using both upper and
                    lower GDD thresholds.

        NOTE: maxt and mint arguments may be either a single float or int or
              a NumPy array of floats or ints. 

        Returns
        --------------------------------------------------------------------
        calculated average temperature of same type and size as input temps
        """
        avgt = self.calcAvgTemp(maxt, mint)
        return self.calcGDD(avgt, gdd_thresholdthreshold)
GDDCalculatorMixin = CallableGDDCalculator

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class GDDCalculator(GDDCalculatorMethods, object):

    def __init__(self, low_threshold, high_threshold=None):
        if high_threshold is None:
            self.threshold = low_threshold
        else: self.threshold = (low_threshold, high_threshold)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def __call__(self, mint, maxt):
        avgt = self.calcAvgTemp(maxt, mint)
        return self.calcGDD(avgt, self.threshold)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# The following classes are DEPRECATED provided for compatabillity with
# previous versions. DO NOTUSE --- THEY WILL BE DELETED VERY SOON.
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# clone of GDDCalculator ... provides consistency with other modules
# that require different methods ofr handling arrays and 3D grids
class ArrayGDDCalculator(GDDCalculator):
    pass

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# clone of GDDCalculator ... provides consistency with other modules
# that require different methods for handling arrays and 3D grids
class GridGDDCalculator(GDDCalculator):
    pass

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class GDDAccumulator(GDDCalculatorMethods, object):

    def __init__(self, low_threshold, high_threshold=None,
                       previously_accumulated_gdd=None):
        if high_threshold is None:
            self.threshold = low_threshold
        else: self.threshold = (low_threshold, high_threshold)
        self.accumulated_gdd = previously_accumulated_gdd
        self.daily_gdd = None

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def __call__(self, mint, maxt, axis=0):
        avgt = self.calcAvgTemp(maxt, mint)
        daily_gdd = self.calcGDD(avgt, self.threshold)
        if self.daily_gdd is not None:
            self.daily_gdd = N.vstack((self.daily_gdd, daily_gdd))
        else: self.daily_gdd = daily_gdd

        accumulated_gdd = self.accumulate(daily_gdd, axis)
        if self.accumulated_gdd is not None:
            self.accumulated_gdd = \
                 N.vstack((self.accumulated_gdd, accumulated_gdd))
        else: self.accumulated_gdd = accumulated_gdd

        return daily_gdd, accumulated_gdd

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def accumulate(self, daily_gdd, axis=0):
        if self.accumulated_gdd is not None:
            return self.accumulateGDD(daily_gdd, axis) + \
                   self._previouslyAccumulated(daily_grid.shape)
        else: return self.accumulateGDD(daily_gdd, axis)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _previouslyAccumulated(self, grid_shape):
        if self.accumulated_gdd is None:
            return N.zeros(grid_shape[1:], dtype=float)
        else:
            if self.accumulated_gdd.ndim == 2:
                return self.accumulated_gdd
            else: return self.accumulated_gdd[-1,:,:]


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# clone of GDDAccumulator ... provides consistency with other modules
# that require different methods ofr handling arrays and 3D grids
class ArrayGDDAccumulator(GDDAccumulator):
    pass

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# clone of GDDAccumulator ... provides consistency with other modules
# that require different methods for handling arrays and 3D grids

class GridGDDAccumulator(GDDAccumulator):
    pass

