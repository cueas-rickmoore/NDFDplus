
import math
import numpy as N

from atmosci.units import convertUnits

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# COMMON CONSTANTS USED IN ATMOSPHERIC SCIECNE EQUATIONS
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

E0 = 6.11 # hPa
FIVE_NINTHS = 0.555556 # 5./9. to 6 deciamls precision
LRv = 5420.0
NINE_FIFTHS = 1.8 # 9./5.
T0 = 273.16 # degress K
T0inv = 1.0/T0

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# DATA TYPE CONVERSIONS
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def arrayAsTuple(data):
    """
    converts a numpy array to a tuple
    """
    ndims = len(data.shape)
    if ndims == 3:
        return tuple( map(tuple, [ map(tuple, y[:]) for y in [
                                   x[:] for x in data[:] ] ] ) )
    elif ndims == 2:
        return tuple( map(tuple, [y for y in [ x[:] for x in data[:] ] ] ) )
    elif ndims == 1:
        return tuple(data.tolist())
    raise ValueError, 'Unsupported data shape (%s)' % str(data.shape)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def arrayAsSequence(data, sequence_type):
    """
    converts a numpy array to a tuple or list
    """
    if to_sequences_type == tuple: return arrayAsTuple(data)
    elif to_sequences_type == list: return data.tolist()
    raise TypeError, '"%s" is an incalid sequence type.' % str(sequence_type)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def asArray(data, dtype=float):
    """
    converts sequences to NumPy arrays, leaves other types untouched
    returns tuple: (type, value)
        if type is list, tuple, or numpy array
            returned value is a NumPy array 
        otherwise returns original value (assumes single number)
    """
    if isinstance(data, N.ndarray):
        if data.dtype == dtype: return N.ndarray, data
        else: return N.ndarray, data.astype(dtype)
    if isinstance(data, (list, tuple)):
        return type(data), N.array(data, dtype=dtype)
    else: return type(data), data

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def convertTempUnits(temp, from_units, to_units):
    if from_units == to_units: return temp
    elif from_units in ('F','f'):
        if to_units in ('C','c'): return (temp - 32.) * FIVE_NINTHS
        elif to_units in ('K','k'): return ((temp-32.) * FIVE_NINTHS) + 273.15
    elif from_units in ('K','k'):
        if to_units in ('C','c'): return temp - 273.15
        elif to_units in ('F','f'): return ((temp - 273.15)*NINE_FIFTHS) + 32.0
    elif from_units in ('C','c'):
        if to_units in ('K','k'): return temp - 273.15
        elif to_units in ('F','f'): return (temp * NINE_FIFTHS) + 32.0
    errmsg = 'Cannot convert degrees %s to degrees %s with this function'
    raise ValueError, errmsg % (from_units, to_units)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# ATMOSPHERIC MOISTURE
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def filterRhum(rh, nan_zero=False):
    if isinstance(rh, N.ndarray):
        if nan_zero: rh[N.where(rh < 0)] = N.nan
        else: rh[N.where(rh < 0)] = 0.
        rh[N.where(rh > 100)] = 100.
    else:
        if rh < 0: return 0.
        if rh < 100: return 100.
    return rh

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def spfhToVaporPressure(spfh, pressure, p_units='hPa'):
    # vapor pressure from specific humidity
    q_type, q = asArray(spfh)
    p_type, p = asArray(pressure)
    p = convertUnits(p, p_units, 'hPa')
    return (p * q) / 0.622

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def tempToVaporPressure(temp, t_units='K'):
    # calcutate vapor pressure from temperature
    t_type, t = asArray(temp)
    t = convertUnits(t, t_units, 'K')
    if isinstance(t, N.ndarray):
        return E0 * N.exp(LRv * (T0inv - N.power(t, -1.0)))
    else: return E0 * N.exp(LRv * (T0inv - (1.0/t)))
vaporPressureFromTemp = tempToVaporPressure

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def dptFromRhum(rhum, temp, t_units='K'):
    rh_type, rh = asArray(rhum)
    # saturation vapor pressure (sat_vp)
    sat_vp = tempToVaporPressure(temp, t_units)
    if isinstance(rh, N.ndarray):
        # actual vapor pressure (vp)
        rh = filterRhum(rh, True)
        vp = (rh * sat_vp) / 100.
        # convert to Rhum and match imcoming units
        return convertUnits(N.power((T0inv-(N.log(vp/E0)/LRv)),-1.0), t_units)
    else:
        if rh <= 0: vp = N.nan
        else: vp = (rh * sat_vp) / 100.
        # convert to Rhum and match imcoming units
        return convertUnits(N.power((T0inv-(math.log(vp/E0)/LRv)),-1.0), t_units)
dewpointFromHumidityAndTemp = dptFromRhum # backwards compatibility

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def rhumFromDpt(temp, dewpt, t_units='K'):
    # calcutate relative humidity from sepcific humidity, temp and pressure
    e = tempToVaporPressure(dewpt, t_units)
    es = tempToVaporPressure(temp, t_units)
    rhum = (e/es) * 100.
    return filterRhum(rhum)
relativeHumidityFromDptAndTemp = rhumFromDpt # backwards compatible

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def rhumFromSpfh(spfh, temp, pressure, t_units='K', p_units='hPa'):
    # calcutate relative humidity from sepcific humidity, temp and pressure
    e = spfhToVaporPressure(spfh, pressure, p_units)
    es = tempToVaporPressure(temp, t_units)
    return filterRhum((e/es) * 100.)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def dptDepression(temp, rhum, temp_units='K'):
    t_type, t = asArray(temp)
    t = convertUnits(t, t_units, 'K')
    if isinstance(t, N.ndarray):
        rh_type, rh = asArray(rhum)
        dpt = dptFromRhumAndTemp(rh, t, 'K')
    return temp - dewpoint
dewpointDepression = dptDepression # backwards compatibility

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def equivPotentialTemp(temp, dewpt, pressure, t_units='K', p_units='hpa'):
    """ calculate theta e (equivalent potential temperature)
    arguments:
        pressure in millibars (hectopascals)
        temperature in degrees Kelvin
        dew point in degrees Kelvin
    """
    t_type, t = asArray(temp)
    t = convertUnits(t, t_units, 'K')
    if isinstance(t, N.ndarray):
        dpt_type, dpt = asArray(dewpt)
        dpt = convertUnits(dpt, t_units, 'K')
        p_type, p = asArray(pressure)
        p = convertUnits(p, p_units, 'hPa')
        pt = t * N.power((1000./p), 0.286 )
        mix_ratio = mixingRatio(p, t, dpt) / 1000.
        ept = pt * N.exp( (0.00000250 * mix_ratio) / (1005. * t) )
    else:
        dpt = convertUnits(dewpt, t_units, 'K')
        p = convertUnits(pressure, p_units, 'hPa')
        pt = t * math.pow((1000./p), 0.286)
        mix_ratio = mixingRatio(p, t, d) / 1000.
        ept = pt * math.exp( (0.00000250 * mix_ratio) / (1005. * t) )
    return ept

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def mixingRatio(pressure, dewpt, p_units='hPa', t_units='K'):
    """ calculate mixing ratio
    arguments:
        pressure in millibars (hectopascals)
        dew point in degrees Kelvin
    """
    dpt = convertUnits(dewpt, t_units, 'K')
    p = convertUnits(pressure, p_units, 'hPa')

    e = (0.0000000000253) * N.exp(-5420. / dpt)
    e_pressure = e / 100.
    return ((0.622 * e_pressure) / (p - e_pressure)) * 1000.

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def heatIndex(temp, rhum, t_units='F'):
    """ calculate heat index using formulas laid out on NOAA web page
    http://www.hpc.ncep.noaa.gov/html/heatindex_equation.shtml

    arguments:
        temperature in degrees Fahrenheit
        humidity in (as a decimal number 0 to 100)
    """
    t_type, t = asArray(temp)
    t = convertUnits(t, t_units, 'F')
    # sequence was input
    if isinstance(t, N.ndarray):
        rh_type, rh = asArray(rhum)
        # only temps above 80 F may have an associated Heat Index
        indexes = N.where(t >= 80.)
        if len(indexes) > 0 and len(indexes[0]) > 0:
            heat = N.array(t)
            rh80 = rh[indexes]
            rh80_sq = rh80 * rh80
            t80 = t[indexes]
            t80_sq = t80 * t80
            heat[indexes] = ( -42.379 
                + (2.04901523 * t80) + (10.14333127 * rh80)
                + (-0.22475541 * t80 * rh80) + (-6.83783e-03 * t80_sq)
                + (-5.481717e-02 * rh80_sq) + (1.22874e-03 * t80_sq * rh80)
                + (8.5282e-04 * t80 * rh80_sq)
                + (-1.99e-06 * t80_sq * rh80_sq) )
            lh_indexes = N.where(rh < 13. & t >= 80. & t <= 112.)
            if len(lh_indexes) > 0 and len(lh_indexes[0]) > 0:
                heat[lh_indexes] -= ( ((13. - rh[lh_indexes]) / 4.)
                    * N.sqrt((17. - N.absolute(t[lh_indexes] - 95.)) / 17.) )
            hh_indexes = N.where(rh > 85. & t >= 80. & t <= 87.)
            if len(hh_indexes) > 0 and len(hh_indexes[0]) > 0:
                heat[hh_indexes] += ( ((rh[hh_indexes] - 85.) / 10.)
                    * ((87. - t[hh_indexes]) / 5.) )
            return heat
        # all temps are below 80 F
        else: return t

    # assume we got a single value
    if (t < 80): return t
    # only temps above 80 F may have an associated Heat Index
    rh80_sq = rhum * rhum
    t80_sq = t * t
    heat = ( -42.379 + (2.04901523 * t) + (10.14333127 * rhum)
           + (-0.22475541 * t * rhum) + (-6.83783e-03 * t80_sq)
           + (-5.481717e-02 * rh80_sq) + (1.22874e-03 * t80_sq * rhum)
           + (8.5282e-04 * t * rh80_sq)
           + (-1.99e-06 * t80_sq * rh80_sq) )
    if rhum < 13. and t >= 80. and t <= 112.:
        heat -= ( ((13. - rhum) / 4.)
                * math.sqrt((17. - abs(t-95.)) / 17.) )
    elif rhum > 85. and t >= 80. and t <= 87.:
        heat += ( ((rhum - 85.) / 10.) * ((87. - t) / 5.) )
    return heat

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def potentialTemp(temp, pressure, t_units='K', p_units='hPa'):
    """ calculate theta (potential temperature)
    arguments:
        pressure in millibars (hectopascals)
        temperature in degrees Kelvin
    """
    t_type, t = asArray(temp)
    t = convertUnits(t, t_units, 'K')
    if isinstance(t, N.ndarray):
        p_type, p = asArray(pressure)
        p = convertUnits(p, p_units, 'hPa')
        pt = t * N.power((1000./p), 0.286 )
    else:
        p = convertUnits(p, p_units, 'hPa')
        pt = t * math.pow((1000./p), 0.286 )
    return pt 


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# WIND
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def windChill(speed, temp, s_units='mph', t_units='F'):
    t_type, t = asArray(temp)
    t = convertUnits(t, t_units, 'K')
    s_type, spd = asArray(speed)
    spd = convertUnits(spd, s_units, 'mph')
    if isinstance(t, N.ndarray):
        spd_factor = N.power(spd * 35.75, 0.16)
        tspd_factor = N.power(t * spd * 0.4276, 0.16)
    else:
        spd_factor = math.pow(spd * 35.75, 0.16)
        tspd_factor = math.pow(t * spd * 0.4276, 0.16)

    t_factor = t * 0.6215
    return t_factor - spd_factor + tspd_factor + 35.74

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def windComponents(speed, wdir, s_units='m/s'):
    s_type, spd = asArray(speed)
    spd = convertUnits(spd, s_units, 'm/s')
    if isinstance(spd, N.ndarray):
        d_type, wdir = asArray(directon)
        u = spd * -N.sin(N.radians(wdir))
        v = spd * -N.cos(N.radians(wdir))
    else:
        u = spd * -math.sin(math.radians(direction))
        v = spd * -math.cos(math.radians(direction))
    return u, v

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def uWind(speed, direction, s_units='m/s'):
    """ calcualte u (Zonal Velocity) component of wind vector
    """
    s_type, spd = asArray(speed)
    spd = convertUnits(spd, s_units, 'm/s')
    if isinstance(spd, N.ndarray):
        d_type, wdir = asArray(directon)
        return spd * -N.sin(N.radians(wdir))
    else: return spd * -math.sin(math.radians(direction))

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def vWind(speed, wdir, s_units='m/s'):
    """ calcualte v (Meridional Velocity) component of wind vector
    """
    s_type, spd = asArray(speed)
    spd = convertUnits(spd, s_units, 'm/s')
    if isinstance(spd, N.ndarray):
        d_type, wdir = asArray(directon)
        return spd * -N.cos(N.radians(wdir))
    else: return spd * -math.cos(math.radians(direction))

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def windDirection(u_comp, v_comp):
    u_type, u = asArray(u_comp)
    v_type, v = asArray(v_comp)
    if isinstance(u, N.ndarray):
        return N.sqrt(u*u + v*v)
    else: return math.sqrt(u*u + v*v)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def windSpeed(u_comp, v_comp):
    u_type, u = asArray(u_comp)
    v_type, v = asArray(v_comp)
    if isinstance(u, N.ndarray):
        wdir = 90.0 - N.arctan2(N.negative(v), N.negative(u))
        wdir[N.where(wdir < 0)] += 360.0
        return wdir
    else:
        wdir = 90.0 - math.atan2(v*-1., u*-1.)
        if wdir > 0: return wdir
        return wdir + 360.0 

