
from dateutil.relativedelta import relativedelta
ONE_DAY = relativedelta(days=1)
import numpy as N

from .array import LinvillArrayModel

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class Linvill3DGridModel(LinvillArrayModel):
    """ Class to calculate the length of a day (or group of days) and
    estimate hourly temperatures at each latitude in a grid of latitudes.
    
    Based on the model described in :
        Linvill, Dale E. (1990), "Calculating Chilling Hours and Chill
        Units from Daily Maximum and Minimum Temperature Observations"
        HORTSCIENCE, 25(l), 14-16.
    """

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def dayLengthArray(self, first_date, last_date, rad_lats):
        """ Determine the number of hours of daylight for each date in a
        range of dates at each node in a 2D grid of latitudes.

        Arguments
        =========
        first_date : datetime.date or datetime.datetime
                     first calendar date in range
        last_date : datetime.date or datetime.datetime
                    last calendar date in range
        rad_lats : 2D numpy array, dtype=float
                   grid of latitudes in radians

        Returns
        =======
        3D numpy array, dtype=int, shape=(num days, rad_lats.shape)
                length of day for each date at each grid node.
        """
        # convert date to factored climatoligcal day
        clim_days = \
        self.climatologicalDayGrid(first_date, last_date, rad_lats.shape)

        # daylight hours
        daylens = N.zeroslike(clim_days)

        # adjust grid nodes > 40 degrees latitude
        nodes = N.where(rad_lats > LAT40_RADS)
        if len(nodes[0]) > 0:
            daylens[nodes] = \
            self._daylightAtLatsGT40(clim_day_factor,rad_lats[nodes])

        # adjust grid nodes <= 40 degrees latitude
        nodes = N.where(rad_lats <= LAT40_RADS)
        if len(nodes[0]) > 0:
            daylens[nodes] = \
            self._daylightAtLatsGT40(clim_day_factor,rad_lats[nodes])

        # drop the decimal hours before returning
        return daylens.astype(int)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def climatologicalDayGrid(self, first_date, last_date, grid_shape):
        clim_days = tuple(climatologicalDaysInRange(first_date, last_date))
        template_grid = N.array(grid_shape, dtype=float)
        clim_day_grid = \
        N.array((clim_days.size,) + grid_shape, dtype=float)

        for indx, clim_day in enumerate(clim_days):
            template_grid.fill(clim_day)
            clim_day_grid[indx] = template_grid
        return clim_day_grid

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def tempGridsToHourly(self, start_date, lats, maxt, mint, units='F'):
        hourly_shape = (maxt.shape[0], 24) + maxt.shape[1:]
        print maxt.shape, hourly_shape
        hourly_grid = N.empty(hourly_shape, dtype=float)
        date = start_date
        for day in range(maxt.shape[0]):
            print 'day', day
            daylen = self.dayLength(date, lats[day])
            hourly_grid[day] = \
                self.hourlyTempsForDay(daylen, maxt[day], mint[day], units)
            date += ONE_DAY
        return hourly_grid

