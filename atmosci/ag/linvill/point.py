
import math
from datetime import datetime
import numpy as N

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def tempRangeToHourly(date, lat, min_t, max_t, units='F', debug=False):
    if units == 'F':
        _mint = (float(min_t) - 32) * 5 / 9
        _maxt = (float(max_t) - 32) * 5 / 9
        if debug: print 'mint, maxt as degrees C', _mint, _maxt
    else:
        _mint = min_t
        _maxt = max_t

    if isinstance(date, datetime):
        month = date.month
        day = date.day
    else:
        month = date[1]
        day = date[2]

    rad_lat = lat * math.pi / 180
    # determine climatological day
    # days numbered from March 1 and all months are 30.6 days long
    clim_day = int(30.6 * month + day + 1 - 91.3)
    if debug: print 'climatological day', clim_day

    # daylight hours
    if lat > 40:
        daylight_hours = int( 12.25 +
                              ( ( 1.6164 + 1.7643 * math.tan(rad_lat)**2 ) *
                              ( math.cos(0.0172 * clim_day - 1.95) ) ) )
    else:
        daylight_hours = int( 12.14 + ( 3.34 * math.tan(rad_lat) *
                                        math.cos(0.0172 * clim_day - 1.95) ) )
    if debug: print 'daylight hours', daylight_hours

    daylight_plus_4 = daylight_hours + 4
    # interpolate daytime temperatures
    day_temps = [ (_maxt - _mint) *
                  math.sin(math.pi*(t+1) / daylight_plus_4) + _mint
                  for t in range(daylight_hours-1) ]
    day_temps.insert(0, _mint)

    if debug:
        print '\ndaylight temps'
        print day_temps
        print 'hourly heating'
        _day_temps = N.array(day_temps)
        print _day_temps[1:] - _day_temps[:-1]
        del _day_temps

    # night time hours
    night_hours = 24 - daylight_hours
    # interpolate night time temperatures
    sunset_temp = day_temps[-1]
    degrees_per_hour = (sunset_temp - _mint) / night_hours
#    night_temps = [ sunset_temp - (degrees_per_hour * (t-daylight_hours))
#                    for t in range(daylight_hours+1, 25)]
    night_temps = [ sunset_temp - (degrees_per_hour * math.log(t+1))
                    for t in range(night_hours)]

    if debug:
        print '\nnight time temps'
        print night_temps
        print 'hourly cooling'
        _night_temps = N.array(night_temps)
        print _night_temps[1:] - _night_temps[:-1]
        del _night_temps

    day_temps.extend(night_temps)
    return N.array(day_temps)

