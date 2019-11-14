
import numpy as N

from .model import LinvillDaylenModel

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from .model import LAT40_RADS # 40 degrees latitude converted to radians

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class LinvillArrayModel(LinvillDaylenModel):
    """ Class to calculate the length of a day (or group of days) and
    estimate hourly temperatures at each latitude in an array of
    latitudes.
    
    Based on the model described in :
        Linvill, Dale E. (1990), "Calculating Chilling Hours and Chill
        Units from Daily Maximum and Minimum Temperature Observations"
        HORTSCIENCE, 25(l), 14-16.
    """

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def dayLengthArray(self, date, rad_lats):
        """ Determine the number of hours of daylight for each location
        in an array of latitudes.

        Arguments
        =========
        date : datetime.date or datetime.datetime
               full calendar date
        rad_lats : 1D numpy array, dtype=float
                   multiple latitudes in radians

        Returns
        =======
        1D numpy array, dtype=int : length of day (same shape as rad_lats)
        """
        # convert date to climatoligcal day
        clim_day = self.climatologicalDay(date)

        # init day length array
        daylens = N.zeros(rad_lats.shape, dtype=int)

        # adjust grid nodes > 40 degrees latitude
        nodes = N.where(rad_lats > LAT40_RADS)
        if len(nodes[0]) > 0:
            daylens[nodes] = \
            self._daylightAtLatsGT40(clim_day, rad_lats[nodes])

        # adjust grid nodes <= 40 degrees latitude
        nodes = N.where(rad_lats <= LAT40_RADS)
        if len(nodes[0]) > 0:
            daylens[nodes] = \
            self._daylightAtLatsGT40(clim_day, rad_lats[nodes])

        # drop the decimal hours before returning
        return daylens

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def hourlyTempsForDays(self, daylens, maxt, mint, units='F'):
        """ Estimate the temperature during each hour or the day
        at multiple locations.

        Arguments
        =========
        daylens : 1D numpy array, dtype=int 
                  number of hours in the day at each location
        maxt : 1D numpy array
               maximum temprature for the day at each location
        mint : 1D or 2D numpy array
               minimum temprature for the day at each location
        units : str
                units for input temperatures.
                must be one of 'F' for Fahrenheit, 'C' for Celsius
        NOTE: daylen, maxt and mint arrays must all be the same size 

        Returns
        =======
        2D numpy array, dtype=float, shape=(daylen.size, 24)
              estimated temperatures for 24 hours at each location

        NOTE: calculations are done in degrees Celsius and returned
              temperatures are always in Celsius
        """
        daylens_plus_4 = daylens + 4

        _maxt, _mint = self._tempsAsCelsius(maxt, mint, units)
        temp_diff = _maxt - _mint

        # need 24 hourly temps for each entry in temp arrays
        hourly_temps = N.zeros((daylen.size, 24), dtype=float)
        # Linvill assumes min temp occurs at first hour of the day
        hourly_temps[:][0] = _mint

        # first step is to interpolate hourly temps through the minimum
        # daylen at any node in the array
        min_daylen = daylen.min()
        for hour in range(1,min_daylen):
            hour_factors = N.sin( (N.pi * hour) / daylen_plus_4 )
            hourly_temps[:][hour] = _mint + (temp_diff * hour_factors)

        # need to track the temperature at sunset for each node
        # sunset_temp will be used to calculate night time temperatures
        # start with a copy of hourly temps at minumum daylen hour 
        sunset_temps = N.copy(hourly_temps[:][min_daylen-1])

        # next step is to fill in hourly temps for any entries where
        # the daylen is more than the minimum daylen
        max_daylen = daylen.max()
        if max_daylen > min_daylen:
            for hour in range(min_daylen, max_daylen):
                days = N.where(daylen >= hour)
                # hour_factors = N.sin((N.pi * hour) / daylen_plus_4[days])
                # needs adjusting to get the daylen array up front
                hour_factors = daylen_plus_4[days]**-1. * N.sin(N.pi * hour)
                temps = _mint[days] + (temp_diff[days] * hour_factors)
                hourly_temps[days][hour] = temps
                sunset_temps[days] = temps

        # finally, interpolate temps during night time hours
        # this part is a gross assumption that the temperature decay
        # rate is the difference between the temperature at sunset and
        # the minumum temperature divided by the number of hours left
        # in the day
        degrees_per_hour = (sunset_temps - _mint) / (24. - daylen)

        # fill in hourly temps at nodes where the daylen is
        # less than the maximum daylen  
        if max_daylen > min_daylen:
            for hour_indx in range(min_daylen, max_daylen):
                hour = (hour_indx - min_daylen) + 1
                days = N.where(hour > daylen)
                hourly_temps[days][hour_indx] = \
                sunset_temps[days] - (degrees_per_hour[days] * N.log(hour))
        # now that all nodes are caught up to the same hour
        # interpolate hourly temps for the rest of the night
        for hour_indx in range(max_daylen, 24):
            hour = (hour_indx - min_daylen) + 1
            hourly_temps[:][hour_indx] = \
            sunset_temps - (degrees_per_hour * N.log(hour))

        return hourly_temps

