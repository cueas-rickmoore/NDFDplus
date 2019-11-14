import numpy as N
import math
PI = math.pi

def linvill(date, lat, t_min, t_max, units='F', debug=False):
    latr = lat *PI / 180    ###convert lat to radians
    if units == 'F':
        t_max_C = (t_max - 32) * 5 / 9
        t_min_C = (t_min - 32) * 5 / 9
        if debug: print 'mint, maxt as degrees C', t_min_C, t_max_C
    else:
        t_max_C = t_max
        t_min_C = t_min

    if isinstance(date, (tuple,list)):
        CD = int(30.6 * date[1] + date[2] + 1 - 91.3)
    else: CD = int(30.6 * date.month + date.day + 1 - 91.3)
    if debug: print 'climatological day', CD

    DL = int( 12.25 + ( ( 1.6164 + 1.7643*math.tan(latr)**2 ) * 
                      ( math.cos(0.0172*CD - 1.95) ) ) )
    if debug: print 'daylight hours', DL

    ### DAY TIME VALUES
    temps =[t_min_C,]
    for k in range(DL-1):
        temp = (t_max_C - t_min_C) * math.sin((PI*(k+1)) / (DL+4)) + t_min_C
        temps.append(temp)
    if debug:
        print '\ndaylight temps'
        print temps
        print 'hourly heating' 
        print N.array(temps[1:]) - N.array(temps[:-1])

    #### APPEND NIGHT TIME VALUES TO DAYTIME LISTS
    NT = 24-DL
    sunset_temp = temps[-1]
    degrees_per_hour = (sunset_temp - t_min_C) / NT
    if debug: print '\nnight time\ndegrees per hour', degrees_per_hour
    night = [ ]
    for k in range(NT):
        temp = sunset_temp - (degrees_per_hour * math.log(k+1))
        night.append(temp)
    if debug:
        print 'night time temps'
        print night
        print 'hourly cooling' 
        print N.array(night[1:]) - N.array(night[:-1])

    temps.extend(night)
    return N.array(temps)

