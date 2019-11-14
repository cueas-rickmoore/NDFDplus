
import numpy as N

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class GDDCalculatorMethods:
    """ Collection of inheritable methods for calculating GDD (Growing
    Degree Days) from temperature data contained in NumPy arrays
    """

    def accumulateGDD(self, daily_gdd, axis=0, prev_accum=None):
        """ Calculate accumulated GDD from a NumPy array of daily GDD.

        Arguments
        --------------------------------------------------------------------
        daily_gdd  : NumPy float array of daily GDD
        axis       : axis along which GDD is to be calculated.
                     NOTE: not used for 1D arrray and defaults to 0 for
                           multi-dimension arrays.
        prev_accum : Numpy float array containing previously accumulated
                     GDD. Must be the same size as "axis" in daily_gdd.
                     Defaults to None.

        Returns
        --------------------------------------------------------------------
        NumPy array af the same dimensions as the input gdd array.
        """
        if daily_gdd.ndim > 1:
            accum_gdd = N.cumsum(daily_gdd, axis=axis)
        else: accum_gdd = N.cumsum(daily_gdd, axis=None)
        if prev_accum is not None:
            aaccum_gdd = accum_gdd + prev_accum
        return accum_gdd

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
        NumPy array with 1 fewer dimensions than the input gdd array.
        """
        if gdd_array.ndim > 1:
            return self.round(N.nanmean(gdd_array, axis=axis))
        else: return self.round(N.nanmean(gdd_array))

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def calcAvgTemp(self, maxt, mint):
        """ Calculate the average temperature 

        Arguments
        --------------------------------------------------------------------
        maxt : NumPy float array containing maximum temperature
        mint : NumPy float array containing minimum temperature

        Returns
        --------------------------------------------------------------------
        calculated average temperature of same type and size as input temps
        """
        return self.round((maxt + mint) * 0.5)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def calcGDD(self, avgt, threshold):
        """ Calculate the GDD using average temperature based on commonly
        accepted GDD threshold rules.

        Arguments
        --------------------------------------------------------------------
        avgt      : NumPy float array containing average temperatures
        threshold : GDD threshold specification. Pass a single int for 
                    calclutations using only a low temperature threshold.
                    Pass a tuple/list for calculations using both upper and
                    lower GDD thresholds.

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

    def round(self, numpy_array, factor=0.01):
        """ round all values in array after adding a "fudge" factor

        Arguments
        --------------------------------------------------------------------
        numpy_array : NumPy float array
        factor      : "fudge" factor to ba applied before rounding
                      used to overcome inconsistencies between Python and
                      NumPy in the way rounding is calculated.

        Returns
        --------------------------------------------------------------------
        NumPy float array containg the rounded data
        """
        return N.round(numpy_array + factor)
    roundGDD = round

#GDDFunctionsMixin = GDDCalculatorMethods # backwards compatible


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class GDDCalculator(GDDCalculatorMethods, object):
    """ A fully functional, callable GDD calculator.
    
    USAGE:

        from atmosci.ag.gddcalc import GDDCalculator
        calc = GDDCalculator()
        gdd = calc(maxt,mint,threshold)

    NOTE: It also inherits the full functinality of GDDCalculatorMethods
    so you can do:

        accum_gdd = calc.accumulateGDD(calc(maxt,mint,threshold),axis=0)
    """

    def __call__(self, maxt, mint, threshold):
        """ Calculate growing degree days from max & min temperatures
        using on commonly accepted GDD threshold rules.

        Arguments
        --------------------------------------------------------------------
        maxt      : NumPy float array containing maximum temperature
        mint      : NumPy float array containing minimum temperature
        threshold : GDD threshold specification. Pass a single int for 
                    calculatations using only a low temperature threshold.
                    Pass a tuple/list for calculations using both upper
                    and lower GDD thresholds.

        Returns
        --------------------------------------------------------------------
        NumPy array (of same type and size as input arrays) that contains
        the calculated GDD at each node
        """
        return self.calcGDD(self.calcAvgTemp(maxt, mint), threshold)

