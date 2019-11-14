""" Unit conversion utilities
"""

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

FIVE_NINTHS = 5./9.
NINE_FIFTHS = 9./5.
                    
CONVERSION_FUNCS = {
    # temperature conversions
    'C_to_F'         : lambda x : (x * NINE_FIFTHS) + 32.
    , 'C_to_K'       : lambda x : x + 273.15
    , 'C_to_R'       : lambda x : (x * 1.8) + 491.67
    , 'F_to_C'       : lambda x : (x - 32.) * FIVE_NINTHS
    , 'F_to_K'       : lambda x : ((x - 32.) * FIVE_NINTHS) + 273.15
    , 'F_to_R'       : lambda x : x + 459.67
    , 'K_to_C'       : lambda x : x - 273.15
    , 'K_to_F'       : lambda x : ((x - 273.15) * NINE_FIFTHS) + 32.
    , 'K_to_R'       : lambda x : x * 1.8
    , 'R_to_C'       : lambda x : (x - 491.67) * 0.555556
    , 'R_to_F'       : lambda x : x - 459.67
    , 'R_to_K'       : lambda x : x * 0.555556
    # unit temperature difference 
    , 'dC_to_dF'     : lambda x : x * NINE_FIFTHS
    , 'dC_to_dK'     : lambda x : x
    , 'dF_to_dC'     : lambda x : x * FIVE_NINTHS
    , 'dF_to_dK'     : lambda x : x * FIVE_NINTHS
    , 'dK_to_dC'     : lambda x : x
    , 'dK_to_dF'     : lambda x : x * NINE_FIFTHS
    # US linear measurement units
    , 'ft_to_in'     : lambda x : x * 12.
    , 'ft_to_mi'     : lambda x : x / 5280.
    , 'in_to_ft'     : lambda x : x * 0.083333
    # metric linear measurement units
    , 'cm_to_km'     : lambda x : x * 0.00001
    , 'cm_to_m'      : lambda x : x * 0.01
    , 'cm_to_mm'     : lambda x : x * 10.
    , 'km_to_cm'     : lambda x : x * 1.0000e+5
    , 'km_to_m'      : lambda x : x * 1000.
    , 'km_to_mm'     : lambda x : x * 1.0000e+6
    , 'm_to_cm'      : lambda x : x * 100.
    , 'm_to_km'      : lambda x : x * 0.001
    , 'm_to_mm'      : lambda x : x * 1000.
    , 'mm_to_cm'     : lambda x : x * 0.1
    , 'mm_to_m'      : lambda x : x * 0.001
    # US linear to metric linear 
    , 'ft_to_cm'     : lambda x : x * 30.480
    , 'ft_to_km'     : lambda x : x * 0.00030480
    , 'ft_to_m'      : lambda x : x * 0.30480
    , 'ft_to_mm'     : lambda x : x * 304.80
    , 'in_to_cm'     : lambda x : x * 2.54
    , 'in_to_m'      : lambda x : x * 0.0254
    , 'in_to_mm'     : lambda x : x * 25.4
    , 'mi_to_km'     : lambda x : x * 1.6093
    # metric linear to US liaenr
    , 'cm_to_in'     : lambda x : x * 0.39370
    , 'cm_to_ft'     : lambda x : x * 0.032808
    , 'km_to_ft'     : lambda x : x * 3280.8399
    , 'km_to_mi'     : lambda x : x * 0.62137
    , 'm_to_ft'      : lambda x : x * 3.2808399
    , 'm_to_in'      : lambda x : x * 39.3701
    , 'm_to_mi'      : lambda x : x * 0.00062137
    , 'mm_to_in'     : lambda x : x * 0.039370
    , 'mm_to_ft'     : lambda x : x * 0.0032808
    # volume
    , 'm3_to_ft3'    : lambda x : x * 35.3147
    , 'ft3_to_m3'    : lambda x : x * 0.0283168
    # humidity
    , 'in_to_kg/m2'  : lambda x : x * 25.4
    , 'kg/m2_to_in'  : lambda x : x / 25.4
    , 'kg/m2_to_mm'  : lambda x : x
    , 'mm_to_kg/m2'  : lambda x : x
    # atmospheric pressure
    , 'hpa_to_atm'    : lambda x : x * 0.000986923
    , 'hpa_to_inHg'   : lambda x : x * 0.02953
    , 'hpa_to_inHg32' : lambda x : x * 0.02953
    , 'hpa_to_inHg60' : lambda x : x * 0.02961
    , 'hpa_to_mb'     : lambda x : x
    , 'mb_to_hpa'     : lambda x : x
    , 'inHg_to_hpa'   : lambda x : x * 33.8639
    , 'inHg32_to_mb'  : lambda x : x * 33.8639
    , 'inHg60_to_hpa' : lambda x : x * 33.7685
    , 'inHg60_to_mb'  : lambda x : x * 33.7685
    , 'mb_to_atm'     : lambda x : x * 0.000986923
    , 'mb_to_inHg'    : lambda x : x * 0.02953
    , 'mb_to_inHg32'  : lambda x : x * 0.02953
    , 'mb_to_inHg60'  : lambda x : x * 0.02961
    # solar radiation
    , 'watt/meter2_to_langley' : lambda x : x * 0.086
    , 'langley_to_watt/meter2' : lambda x : x / 0.086
    # wind
    , 'mph_to_miles/hour' : lambda x : x
    , 'miles/hour_to_mph' : lambda x : x
    , 'ft/s_to_knots'     : lambda x : x * 0.5924838
    , 'ft/s_to_kph'       : lambda x : x * 1.09728
    , 'ft/s_to_mph'       : lambda x : x * 0.681818
    , 'ft/s_to_m/s'       : lambda x : x * 0.3048
    , 'knots_to_ft/s'     : lambda x : x * 1.678099
    , 'knots_to_kph'      : lambda x : x * 1.852
    , 'knots_to_m/s'      : lambda x : x * 0.514444
    , 'knots_to_mph'      : lambda x : x * 1.1507794
    , 'kph_to_ft/s'       : lambda x : x * 0.911344
    , 'kph_to_knots'      : lambda x : x * 0.5399568
    , 'kph_to_mph'        : lambda x : x * 0.621317119
    , 'kph_to_m/s'        : lambda x : x * 0.277778
    , 'mph_to_ft/s'       : lambda x : x * 1.466666
    , 'mph_to_knots'      : lambda x : x * 0.86897624
    , 'mph_to_kph'        : lambda x : x * 1.609344
    , 'mph_to_m/s'        : lambda x : x * 0.44704 
    , 'm/s_to_ft/s'       : lambda x : x * 3.28084
    , 'm/s_to_knots'      : lambda x : x * 1.943846
    , 'm/s_to_kph'        : lambda x : x * 3.6
    , 'm/s_to_mph'        : lambda x : x * 2.2369363
    }

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def convertUnits(data, from_units, to_units):
    if from_units == to_units: return data

    # scaled unit conversions
    if '*' in from_units:
        from_units, from_scale =  from_units.split('*')
        if data.dtype.kind == 'f': data /= float(from_scale)
        elif data.dtype.kind == 'i': data /= int(from_scale)
        if from_units == to_units: return data

    if '*' in to_units: to_units, to_scale = to_units.split('*')
    else: to_scale = None

    if from_units != to_units:
        func = CONVERSION_FUNCS.get('%s_to_%s' % (from_units,to_units), None)
        if func is not None: data = func(data)
        else:
            errmsg = 'Cannot convert %s units to %s units'
            print errmsg % (from_units, to_units)
            raise ValueError, errmsg % (from_units, to_units)

    if to_scale is not None:
        if data.dtype.kind == 'f': data *= float(to_scale)
        elif data.dtype.kind == 'i': data *= int(to_scale)
    return data

def getConversionFunction(from_units, to_units):
    if from_units is not None and to_units is not None:
        def convert(data): return convertUnits(data, from_units, to_units)
        return convert
    return None

def isSupportedUnitConversion(from_units, to_units):
    conversion = '%s_to_%s' % (from_units, to_units)
    return conversion in CONVERSION_FUNCS

