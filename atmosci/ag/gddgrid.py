
import numpy as N

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def gddThresholdAsString(gdd_threshold):
    if isinstance(gdd_threshold, (list,tuple)):
        return ''.join(['%02d' % th for th in gdd_threshold])
    elif isinstance(gdd_threshold, int):
        return '%02d' % gdd_threshold
    elif isinstance(gdd_threshold, basestring):
        return gdd_threshold
    else: return str(gdd_threshold)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class GDDProjectFileMethods:

    def accumulateGDD(self, daily_gdd):
        """ accumulate GDD from a NumPy array of daily GDD values.

        Arguments
        --------------------------------------------------------------------
        daily_gdd  : NumPy array of daily GDD
                     NOTE: must be 3 dimensions with time as first dimension
        axis       : axis along which GDD is to be calculated.

        Returns
        --------------------------------------------------------------------
        NumPy array of accumulated GDD.
        """
        if daily_gdd.ndim > 1: return N.cumsum(daily_gdd, axis=0)
        else: return N.cumsum(daily_gdd, axis=None)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def calcAvgGDD(self, gdd_array, num_years=None):
        """ Calculate the average for a NumPy array of GDD values.

        Arguments
        --------------------------------------------------------------------
        gdd_array : NumPy float array of GDD values
                    if num_years is None, then gdd_array must be 3 dimensions
                    (time, x, y) or (time, y, x)
        num_years : indicates that gdd_array is 2-dmiensional and contains
                    GDD summed over 'num_years' years 

        Returns
        --------------------------------------------------------------------
        NumPy array with 1 fewer dimensions than the input gdd array.
        """
        if num_years == None:
            return N.round(N.nanmean(gdd_array, axis=0) + .01)
        else: return N.round((gdd_array/num_years) + .01) 

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def calcAvgTemp(self, maxt, mint):
        """ Calculate the average temperature 

        Arguments
        --------------------------------------------------------------------
        maxt : NumPy array of maximum temperature
        mint : NumPy array of minimum temperature

        Returns
        --------------------------------------------------------------------
        NumPy array of calculated average temperature
        """
        return N.round(((maxt + mint) * 0.5) + .01)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def calcGDD(self, avgt, threshold):
        """ Calculate the GDD using average temperature based on commonly
        accepted GDD threshold rules.

        Arguments
        --------------------------------------------------------------------
        avgt      : average temperature
                    NOTE: must be NumPy array.
        threshold : GDD threshold specification. Pass a single int for 
                    caclutations using only a low temperature threshold.
                    Pass a tuple/list for calculations using both upper and
                    lower GDD thresholds.

        Returns
        --------------------------------------------------------------------
        array with calculated GDD at each node
        """
        # extract lo/hi thresholds
        if isinstance(threshold, (list,tuple)):
            if threshold[0] > threshold[1]:
                hi_thold, lo_thold = threshold
            else: lo_thold, hi_thold = threshold
        else:
            lo_thold = threshold
            hi_thold = None

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

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def gddData(self, coverage, gdd_threshold, start_date, end_date, **kwargs):
        dataset_path = self.gddDatasetPath(coverage, gdd_threshold)
        return self.timeSlice(dataset_path, start_date, end_date, **kwargs) 
    getGDD = gddData # backwards compatible

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def gddDataAtNode(self, coverage, gdd_threshold, start_date, end_date, 
                            lon, lat, **kwargs):
        dataset_path = self.gddDatasetPath(coverage, gdd_threshold)
        data = self.sliceAtNode(dataset_path, start_date, end_date, lon, lat,
                                **kwargs)
        return data
    getGDDAtNode = gddDataAtNode # backwards compatible

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def gddThresholdAsString(self, gdd_threshold):
        return gddThresholdAsString(gdd_threshold)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def gddDatasetPath(self, coverage, gdd_threshold):
        return '%s.%s' % (self.gddGroupPath(gdd_threshold), coverage.lower()) 

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def gddGroupPath(self, gdd_threshold):
        return 'gdd%s' % self.gddThresholdAsString(gdd_threshold)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def roundUp(self, data):
        """ round all values in data array using GDD rounding criteria
        """
        return N.round(data + .01)

