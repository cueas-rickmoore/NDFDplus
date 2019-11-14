
import math
from datetime import datetime
import numpy as N

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def DayLengthCalculator(object):

    def linvill(self, 
    (date, lats, mint, maxt, units='F', debug=False):
    rad_lats = lats * N.pi / 180
    midpoint = (maxt.shape[0] / 2, maxt.shape[1] / 2)

    if units == 'F':
        _mint = (mint - 32) * 5 / 9
        _maxt = (maxt - 32) * 5 / 9
    else:
        _mint = mint
        _maxt = maxt

    if debug:
        msg = 'Linvill Grid Calculations for %s' % date.strftime('%Y-%m-%d')
        print '\n\n', msg
        print '=' * len(msg)
        y, x = midpoint
        print 'maxt in degrees C @ node[%d,%d] :' % midpoint, _maxt[y,x]
        print 'mint in degrees C @ node[%d,%d] :' % midpoint, _mint[y,x]
        avgt = _mint[y, x] + ((_maxt[y, x] - _mint[y, x]) / 2)
        print 'avgt in degrees C @ node[%d,%d] :' % midpoint, avgt

    # determine climatological day
    # days numbered from March 1 and all months are 30.6 days long
    clim_day = int(30.6 * date.month + date.day + 1 - 91.3)
    if debug: print '\nclimatological day', clim_day
    date_factor = N.cos(0.0172 * clim_day - 1.95)

    # daylight hours
    daylight_hours = N.zeros(lats.shape, dtype=int)
    # adjust grid nodes > 40 degrees latitude
    gt40 = N.where(lats > 40)
    if len(gt40[0]) > 0:
        daylight_hours[gt40] = N.array(
                      ( ( (N.tan(rad_lats[gt40])**2 * 1.7643) + 1.6164 ) *
                      date_factor ) + 12.25, dtype=int)
    # adjust grid nodes <= 40 degrees latitude
    le40 = N.where(lats <= 40)
    if len(le40[0]) > 0:
        daylight_hours[le40] = N.array( ( (N.tan(rad_lats[le40]) * 3.34) *
                                         date_factor ) + 12.14, dtype=int)
    if debug:
        y, x = midpoint
        print 'daylight hours at node[%d:%d] :' % midpoint, daylight_hours[y,x]

    # interpolate daytime temperatures
    interp_temps = N.zeros((24,) + lats.shape, dtype=float)
    interp_temps[0][:,:] = _mint
    temp_diff = _maxt - _mint

    daylight_plus_4 = daylight_hours + 4
    min_daylight_hours = daylight_hours.min()
    for hour in range(1,min_daylight_hours):
        hour_factors = N.sin( (N.pi * hour) / daylight_plus_4 )
        interp_temps[hour][:,:] = _mint + (temp_diff * hour_factors)

    max_daylight_hours = daylight_hours.max()

    # initialize sunset temp grid
    sunset_temps = N.copy(interp_temps[min_daylight_hours-1,:,:])

    if max_daylight_hours > min_daylight_hours:
        for hour in range(min_daylight_hours, max_daylight_hours):
            nodes = N.where(daylight_hours >= hour)
            hour_factors = N.sin((N.pi * hour) / daylight_plus_4[nodes])
            temps = _mint[nodes] + ( temp_diff[nodes] * hour_factors)
            interp_temps[hour][nodes] = temps
            sunset_temps[nodes] = temps
    if debug:
        y, x = midpoint
        mdl = daylight_hours[y,x]
        print 'daylight temps @ node[%d,%d] :' % midpoint
        print interp_temps[:mdl,y,x]
        print 'daylight heating/cooling @ node[%d,%d] :' % midpoint
        print interp_temps[1:mdl,y,x] - interp_temps[:mdl-1,y,x]
        print 'sunset temp @ node[%d,%d] :' % midpoint, sunset_temps[y,x]

    # night time hours
    night_hours = 24 - daylight_hours
    degrees_per_hour = (sunset_temps - _mint) / night_hours
    if debug:
        y, x = midpoint
        print 'night hours @ node[%d,%d] :' % midpoint, night_hours[y,x]
        print 'degrees per hour @ node[%d,%d] :'% midpoint, degrees_per_hour[y,x]

    # interpolate night time temperatures
    if max_daylight_hours > min_daylight_hours:
        for indx in range(min_daylight_hours, max_daylight_hours):
            hour = (indx - min_daylight_hours) + 1
            nodes = N.where(hour > daylight_hours)
            interp_temps[indx][nodes] =\
            sunset_temps[nodes] - (degrees_per_hour[nodes] * N.log(hour))

    for indx in range(max_daylight_hours, 24):
        hour = (indx - min_daylight_hours) + 1
        interp_temps[indx] = sunset_temps - (degrees_per_hour * N.log(hour))

    if debug:
        y, x = midpoint
        nite = daylight_hours[y,x]
        print 'night time temps @ node[%d,%d] :' % midpoint
        print interp_temps[nite:,y,x]
        print 'night time heating/cooling @ node[%d,%d]' % midpoint
        print interp_temps[nite-1:-1,y,x] - interp_temps[nite:,y,x]

    return interp_temps

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def register(registry):
    registry['grid'] = tempGridToHourly
    registry['grid3D'] = temp3DGridToHourly

