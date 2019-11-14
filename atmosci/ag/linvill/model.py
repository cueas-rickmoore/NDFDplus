
import datetime
import math
from dateutil.relativedelta import relativedelta
ONE_DAY = relativedelta(days=1)

import numpy as N

from atmosci.utils.timeutils import dateIterator, isLeapYear

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

dates = tuple( [ (date.month,date.day)
                 for date in dateIterator(datetime.datetime(2001,1,1),
                                          datetime.datetime(2001,12,31)) ] )
months, days = zip(*dates)
CALENDAR_MONTHS = N.array(months)
CALENDAR_DAYS = N.array(days)

dates = tuple( [ (date.month,date.day)
                 for date in dateIterator(datetime.datetime(2004,1,1),
                                          datetime.datetime(2004,12,31)) ] )
months, days = zip(*dates)
LEAP_MONTHS = N.array(months)
LEAP_DAYS = N.array(days)
del dates, days, months

FIVE_NINTHS = 5./9.
NINE_FIFTHS = 9./5.
LAT40_RADS = 0.69813170 # 40 degrees latitude converted to radians

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class ClimDayIterator(object):
    def __init__(self, first_date, last_date):
        self.date = first_date
        self.last_date = last_date

    def __iter__(self): return self

    def next(self):
        if self.date <= self.last_date:
            date = self.date
            self.date = date + ONE_DAY
            if date.month >= 3:
                return int((30.6 * date.month) + date.day - 91.3)
            else: return int((30.6 * (date.month+12)) + date.day - 91.3)
        raise StopIteration

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class LinvillDaylenModel(object):
    """ Class to calculate the length of a day (or group of days) and
    estimate hourly temperatures at a single latitude.

    Based on the model described in :
        Linvill, Dale E. (1990), "Calculating Chilling Hours and Chill
        Units from Daily Maximum and Minimum Temperature Observations"
        HORTSCIENCE, 25(l), 14-16.
    """

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # single day
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def climatologicalDay(self, date):
        """ Determine climatological day for a single calendar date.
        Cilmatological days are numbered from March 1 and all months
        are 30.6 days long.

        Argument
        ========
        date : datetime.date or datetime.datetime
               calendar date

        Returns
        =======
        int : scalar
              climatological day corresponding to the calendar date
        """
        if date.month > 2: # March and later
            return int((30.6 * date.month) + date.day - 91.3)
        else: # January and February
            return int((30.6 * (date.month+12)) + date.day - 91.3)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def dayLength(self, date, lat_rad):
        """ Determine the number of hours of daylight for a single day
        or group of days at a single latitude.

        Arguments
        =========
        date : datetime.date or datetime.datetime : calendar date
        lat_rad  : float, scalar : latitude in radians

        Returns
        =======
        int : length of day
        """
        clim_day = self.climatologicalDay(date)
        if lat_rads > LAT40_RADS:
            return self._daylightAtLatGT40(clim_day, lat_rads)
        # latitude is less than or equal to 40 degrees 
        else: return self._daylightAtLatLE40(clim_day, lat_rads)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def hourlyTempsForDay(self, daylen, maxt, mint, units='F'):
        """ Estimate the temperature at each hour of a single day.

        Arguments
        =========
        daylen : int - scalar
                 number of hours in the day
        maxt : float - scalar
               maximum temprature for the day
        mint : float - scalar
               minimum temprature for the day
        units : str 
                units for input temperatures.
                must be one of 'F' for Fahrenheit, 'C' for Celsius

        Returns
        =======
        float - tuple : estimated temperatures for 24 hours

        NOTE: All calculations are done in degrees Celsius and
        returned temperatures are always in degrees Celsius.
        """
        daylen_plus_4 = daylen + 4
        # interpolate daytime temperatures
        _maxt, _mint = self.maxMinTempAsCelsius(maxt, mint, units)
        temp_diff = _maxt - _mint
        day_temps = [ temp_diff *
                      math.sin(math.pi*(t+1) / daylen_plus_4) + _mint
                      for t in range(daylen-1) ]
        # Linvill assumes min temp occurs at first hour of the day
        day_temps.insert(0, _mint)

        # night time hours
        night_hours = 24 - daylen
        # interpolate night time temperatures
        sunset_temp = day_temps[-1]
        degrees_per_hour = (sunset_temp - _mint) / night_hours
        night_temps = [ sunset_temp - (degrees_per_hour * math.log(t+1))
                        for t in range(night_hours)]

        return tuple(day_temps + night_temps)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # multiple days
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def climatologicalDaysInRange(self, first_date, last_date):
        """ Determine climatological day for each of a sequence of calendar
        dates.
        Cilmatological days are numbered from March 1 and all months
        are 30.6 days long

        Argument
        ========
        first_date : datetime.date or datetime.datetime
                     first date in range
        last_date : datetime.date or datetime.datetime
                    last date in range

        Returns
        =======
        1D numpy array, size=num days in range 
              climatological day corresponding to each date in range
        """
        first_indx = first_date.timetuple().tm_yday - 1
        last_indx = last_date.timetuple().tm_yday
        if isLeapYear(first_date.year):
            months = LEAP_MONTHS[first_indx:last_indx]
            days = LEAP_DAYS[first_indx:last_indx]
        else:
            months = CALENDAR_MONTHS[first_indx:last_indx]
            days = CALENDAR_DAYS[first_indx:last_indx]
        clim_days = N.empty(months.size, dtype=float)

        # January and February are at the end of the Linvill year
        indexes = N.where(months < 3)
        if len(indexes[0]) > 0:
            clim_days[indexes] = \
            ((months[indexes]+12) * 30.6) + days[indexes] - 91.3
        # March is at the beginning of the Linvill year
        indexes = N.where(months > 2) # same result as >= 3 used in paper
        if len(indexes[0]) > 0:
            clim_days[indexes] = \
            (months[indexes] * 30.6) + days[indexes] - 91.3

        return clim_days

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def dayLengths(self, first_date, last_date, lat_rad):
        """ Determine the number of hours of daylight for a range of days
        at a single latitude

        Arguments
        =========
        first_date : datetime.date or datetime.datetime
                     first calendar date range
        last_date : datetime.date or datetime.datetime
                    last calendar date in range
        lat_rad  : float - scalar
                   latitude in radians

        Returns
        =======
        int - tuple : length of each day in range
                      len = number of days
        """
        num_days = (last_date - first_date).days + 1
        clim_day_iterator = ClimDayIterator(first_date,last_date)

        # latitudes are greater than 40 degrees 
        if lat_rad > LAT40_RADS:
            daylens = [ self._daylightAtLatGT40(clim_day, lat_rad)
                        for clim_day in clim_day_iterator ]
        # latitudes are less than or equal to 40 degrees 
        else:
            daylens = [ self._daylightAtLatLE40(clim_day, lat_rad)
                        for clim_day in clim_day_iterator ]
        return tuple(daylens)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def hourlyTempsForDays(self, daylens, maxts, mints, units='F'):
        """ Estimate the temperature at each hour of multiple days.

        Arguments
        =========
        daylen : tuple or list, type=int
                 number of hours in each day
        maxt : tuple or list, type=float
               maximum temprature for each day
        mint : tuple or list, type=float
               minimum temprature for each day
        units : str 
                units for input temperatures.
                must be one of 'F' for Fahrenheit, 'C' for Celsius

        Returns
        =======
        2D numpy array, dtype=float, shape=(number of days, 24)
                Estimated temperatures for 24 hours for each day.

        NOTE: All calculations are done in degrees Celsius and
        returned temperatures are always in degrees Celsius.
        """
        temps = [ self.hourlyTempsForDay(daylen[i], maxt[i], mint[i], units)
                  for i in range(len(daylen)) ]
        return N.array(temps)

    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #
    # utility functions 
    # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - # - - - #

    def celsiusToFahrenheit(self, celsius):
        return (celsius * NINE_FIFTHS) + 32.

    def fahrenheitToCelsius(self, fahrenheit):
        return (fahrenheit - 32.) * FIVE_NINTHS

    def latToRadians(self, lat):
        return (lat * N.pi) / 180.

    def maxMinTempAsCelsius(self, maxt, mint, units):
        if units == 'F':
            return ( self.fahrenheitToCelsius(maxt),
                     self.fahrenheitToCelsius(mint) )
        elif units == 'C':
            return maxt, mint
        else:
            raise ValueError, 'Temperatures units must be "F" or "C"'

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _daylightAtLatGT40(self, clim_day, lat_rad):
        """ Determine the number of daylight hours of for a single day at a
        single latitude greater than 40 degrees North

        Arguments
        =========
        clim_day : int, scalar or 1D numpy array
                   Linvill model climatological day(s)
        lat_rad  : float, scalar
                   latitude in radians

        Returns
        =======
        int, scalar or  : number of whole daylight hours
        """
        daylight = ( N.cos( (clim_day * 0.0172) - 1.95 ) *
                     ( (N.tan(lat_rad)**2 * 1.7643) + 1.6164 ) ) + 12.25
        if isinstance(daylight, N.ndarray):
            return daylight.astype(int)
        else: return int(daylight)

    def _daylightAtLatLE40(self, clim_day, lat_rad):
        """ Determine the number of daylight hours of for a single day at a
        single latitude less than or equal to 40 degrees North

        Arguments
        =========
        clim_day : int, scalar or 1D numpy array
                   Linvill model climatological day(s)
        lat_rad  : float, scalar
                   latitude in radians

        Returns
        =======
        int, scalar : number of whole daylight hours
        """
        daylight = ( N.cos( (clim_day * 0.0172) - 1.95 ) *
                     (N.tan(rad_lats) * 3.34) ) + 12.14
        if isinstance(daylight, N.ndarray):
            return daylight.astype(int)
        else: return int(daylight)

