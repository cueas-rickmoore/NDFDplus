""" Unit conversion utilities
"""
import numpy as N

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

UNIT_KEY_MAP = { 
            'cm2'       : 'cm^2',
            'cm**2'     : 'cm^2',
            'cm3'       : 'cm^3',
            'cm**3'     : 'cm^3',
            'fps'       : 'ft/s',
            'ft2'       : 'ft^2',
            'ft**2'     : 'ft^2',
            'ft3'       : 'ft^3',
            'ft**3'     : 'ft^3',
            'ft/s**2'   : 'ft/s^2',
            'ft/s/s'    : 'ft/s^2',
            'ft s-2'    : 'ft/s^2',
            'ft s**-2'  : 'ft/s^2',
            'in2'       : 'in^2',
            'in**2'     : 'in^2',
            'in3'       : 'in^3',
            'in**3'     : 'in^3',
            'kg cm^-2'  : 'kg/cm^2',
            'kg cm**-2' : 'kg/cm^2',
            'kg/cm**2'  : 'kg/cm^2',
            'kg m^-2'   : 'kg/m^2',
            'kg m**-2'  : 'kg/m^2',
            'kg/m**-2'  : 'kg/m^2',
            'km2'       : 'km^2',
            'km**2'     : 'km^2',
            'km/hr'     : 'kph',
            'knots'     : 'knt',
            'm2'        : 'm^2',
            'm**2'      : 'm^2',
            'm3'        : 'm^3',
            'm**3'      : 'm^3',
            'm s^-2'    : 'm/s^2',
            'm s**-2'   : 'm/s^2',
            'm/s**2'    : 'm/s^2',
            'mi2'       : 'mi^2',
            'mi**2'     : 'mi^2',
            'mi/hr'     : 'mph',
            'mile'      : 'mi',
            'miles'     : 'mi',
            'N/m2'      : 'N/m^2',
            'N/m**2'    : 'N/m^2',
}

FORMULAS = {
    # acceleration
    'ft/s^2_to_m/s^2'   : ('*', 0.30480)
    , 'ft/s^2_to_grav'  : ('*', 0.03108095)
    , 'grav_to_ft/s^2'  : ('*', 32.1740485)
    , 'grav_to_m/s^2'   : ('*', 9.8066499)
    , 'm/s^2_to_ft/s^2' : ('*', 3.280839)
    , 'm/s^2_to_grav'   : ('*', 0.10197162)
    # area
    , 'cm^2_to_ft^2'   : ('*', 0.0010764)
    , 'cm^2_to_in^2'   : ('*', 0.155000)
    , 'cm^2_to_m^2'    : ('*', 0.0001)
    , 'ft^2_to_in^2'   : ('*', 144.0)
    , 'ft^2_to_m^2'    : ('*', 0.092903)
    , 'in^2_to_cm^2'   : ('*', 6.4516)
    , 'in^2_to_ft^2'   : ('*', 0.00694444)
    , 'in^2_to_m^2'    : ('*', 0.00064516)
    , 'km^2_to_ft^2'   : ('*', 3280.8399)
    , 'km^2_to_m^2'    : ('*', 1000000.0)
    , 'km^2_to_mi^2'   : ('*', 0.386102)
    , 'm^2_to_ft^2'    : ('*', 10.763910)
    , 'm^2_to_in^2'    : ('*', 1550.00310)
    , 'mi^2_to_ft^2'   : ('*', 2.788e+7)
    , 'mi^2_to_km^2'   : ('*', 2.58999)
    # atmospheric pressure - atmospheres
    , 'atm_to_bar'     : ('*', 1.01325)
    , 'atm_to_inHg'    : ('*', 29.92)
    , 'atm_to_mb'      : ('*', 1013.25)
    , 'atm_to_hPa'     : ('*', 1013.25)
    , 'atm_to_kg/cm^2' : ('*', 1.0333)
    , 'atm_to_kPa'     : ('*', 101.325)
    , 'atm_to_lb/in^2' : ('*', 14.70)
    , 'atm_to_Pa'      : ('*', 101325.0)
    # atmospheric pressure - hectopascals
    , 'hPa_to_atm'    : ('*', 0.000986923)
    , 'hPa_to_inHg'   : ('*', 0.02953)
    , 'hPa_to_inHg32' : ('*', 0.02953)
    , 'hPa_to_inHg60' : ('*', 0.02961)
    , 'hPa_to_kPa'    : ('*', 0.1)
    , 'hPa_to_mb'     : ('==','x')
    , 'hPa_to_N/m^2'  : ('*', 100.0)
    , 'hPa_to_Pa'     : ('*', 100.0)
    # atmospheric pressure - kiloopascals
    , 'kPa_to_hPa'    : ('*', 10.0)
    , 'kPa_to_N/m^2'  : ('*', 1000.0)
    , 'kPa_to_Pa'     : ('*', 1000.0)
    # atmospheric pressure - inches of mercury
    , 'inHg_to_hPa'   : ('*', 33.8639)
    , 'inHg_to_mb'    : ('*', 33.8639)
    , 'inHg_to_Pa'    : ('*', 3385.399)
    , 'inHg32_to_hPa' : ('*', 33.85399)
    , 'inHg32_to_mb'  : ('*', 33.85399)
    , 'inHg32_to_Pa'  : ('*', 3385.399)
    , 'inHg60_to_hPa' : ('*', 33.7685)
    , 'inHg60_to_mb'  : ('*', 33.7685)
    , 'inHg60_to_Pa'  : ('*', 3376.85)
    # atmospheric pressure - millibars
    , 'mb_to_atm'     : ('*', 0.000986923)
    , 'mb_to_hPa'     : ('==','x')
    , 'mb_to_inHg'    : ('*', 0.029538)
    , 'mb_to_inHg32'  : ('*', 0.029535)
    , 'mb_to_inHg60'  : ('*', 0.02961)
    , 'mb_to_Pa'      : ('*', 100.0)
    # atmospheric pressure - Newtons/m^2
    , 'N/m^2_to_Pa'   : ('==', 'N/m^2')
    , 'N/m^2_to_hPa'  : ('*', 0.01)
    # atmospheric pressure - pascals
    , 'Pa_to_hPa'     : ('*', 0.01)
    , 'Pa_to_kPa'     : ('*', 0.001)
    , 'pa_to_mb'      : ('*', 0.01)
    , 'Pa_to_mb'      : ('*', 0.01)
    , 'pa_to_inHg'    : ('*', 0.000295386)
    , 'Pa_to_inHg'    : ('*', 0.000295386)
    , 'pa_to_inHg32'  : ('*', 0.000295386)
    , 'Pa_to_inHg32'  : ('*', 0.000295386)
    , 'pa_to_inHg60'  : ('*', 0.0002961)
    , 'Pa_to_inHg60'  : ('*', 0.0002961)
    , 'Pa_to_N/m^2'   : ('==', 'Pa')
    # density
    , 'kg/cm3_to_lb/ft3' : ('*', 62427.96084)
    , 'kg/cm3_to_lb/in3' : ('*', 36.127292)
    , 'kg/cm3_to_kg/m3'  : ('*', 1000000.0)
    , 'kg/m3_to_kg/cm3'  : ('*', 0.000001)
    , 'kg/m3_to_lb/ft3'  : ('*', 0.062427961)
    , 'kg/m3_to_lb/in3'  : ('*', 0.000036127292)
    , 'lb/in3_to_lb/ft3' : ('*', 1728.000000017)
    , 'lb/in3_to_kg/cm3' : ('*', 0.027679904)
    , 'lb/in3_to_kg/m3'  : ('*', 27679.904593)
    , 'lb/ft3_to_kg/cm3' : ('*', 0.000016018463)
    , 'lb/ft3_to_kg/m3'  : ('*', 16.0184633)
    , 'lb/ft3_to_lb/in3' : ('*', 0.0005787037)
    # distance - US linear measurement units
    , 'in_to_ft'     : ('*', 0.083333)
    , 'ft_to_in'     : ('*', 12.0)
    , 'ft_to_mi'     : ('*', 0.00018939) # x / 5280.
    , 'mi_to_ft'     : ('*', 5280.)
    # distance - metric linear measurement units
    , 'cm_to_km'     : ('*', 0.00001)
    , 'cm_to_m'      : ('*', 0.01)
    , 'cm_to_mm'     : ('*', 10.0)
    , 'km_to_cm'     : ('*', 1.0000e+5)
    , 'km_to_m'      : ('*', 1000.0)
    , 'km_to_mm'     : ('*', 1.0000e+6)
    , 'km_to_nm'     : ('*', 0.53996)
    , 'm_to_cm'      : ('*', 100.0)
    , 'm_to_km'      : ('*', 0.001)
    , 'm_to_mm'      : ('*', 1000.0)
    , 'mm_to_cm'     : ('*', 0.1)
    , 'mm_to_m'      : ('*', 0.001)
    , 'nm_to_km'     : ('*', 1.852)
    # distance - US linear to metric linear 
    , 'in_to_cm'     : ('*', 2.54)
    , 'in_to_m'      : ('*', 0.0254)
    , 'in_to_mm'     : ('*', 25.4)
    , 'ft_to_cm'     : ('*', 30.480)
    , 'ft_to_km'     : ('*', 0.00030480)
    , 'ft_to_m'      : ('*', 0.30480)
    , 'ft_to_mm'     : ('*', 304.80)
    , 'mi_to_km'     : ('*', 1.60934)
    , 'mi_to_nm'     : ('*', 1.1508)
    # distance - metric linear to US liaenr
    , 'cm_to_in'     : ('*', 0.393701)
    , 'cm_to_ft'     : ('*', 0.032808)
    , 'km_to_ft'     : ('*', 3280.8399)
    , 'km_to_mi'     : ('*', 0.62137)
    , 'm_to_ft'      : ('*', 3.2808399)
    , 'm_to_in'      : ('*', 39.3701)
    , 'm_to_mi'      : ('*', 0.00062137)
    , 'mm_to_in'     : ('*', 0.039370)
    , 'mm_to_ft'     : ('*', 0.0032808)
    , 'nm_to_mi'     : ('*', 0.868961)
    # energy
    , 'cal_to_J'   : ('*', 4.1868)
    , 'cal_to_Nm'  : ('*', 4.1868)
    , 'cal_to_kWh' : ('*', 0.000001163)
    , 'cal_to_Wh'  : ('*', 0.001163)
    , 'J_to_cal'   : ('*', 0.23884589)
    , 'J_to_Nm'    : ('==', 'J')
    , 'J_to_kWh'   : ('*', 2.777777777E-7)
    , 'J_to_Wh'    : ('*', 0.0002777777)
    , 'kWh_to_cal' : ('*', 859845.227859)
    , 'kWh_to_J'   : ('*', 3600000.0)
    , 'kWh_to_Nm'  : ('*', 3600000.0)
    , 'kWh_to_Wh'  : ('*', 1000.0)
    , 'Nm_to_cal'  : ('*', 0.23884589)
    , 'Nm_to_J'    : ('==', 'Nm')
    , 'Nm_to_kWh'  : ('*', 2.777777777E-7)
    , 'Nm_to_Wh'   : ('*', 0.0002777777)
    , 'Wh_to_cal'  : ('*', 859.845227859)
    , 'Wh_to_J'    : ('*', 3600.0)
    , 'Wh_to_kWh'  : ('*', 0.001)
    , 'Wh_to_Nm'    : ('*', 3600.0)
    # kg meter per second^2
    , 'J_to_kg.m2/s2'   : ('==', 'kg.m2/s2')
    , 'Nm_to_kg.m2/s2'  : ('==', 'kg.m2/s2')
    , 'kg.m2/s2_to_J'   : ('==', 'J')
    , 'kg.m2/s2_to_Nm'  : ('==', 'Nm')
    # force
    , 'J/m_to_kg.m/s2'  : ('==', 'J/m')
    , 'J/m_to_lbf'      : ('*', 0.22480894)
    , 'J/m_to_N'        : ('==', 'J/m')
    , 'kg.m/s2_to_j/m'  : ('==', 'kg.m/s2')
    , 'kg.m/s2_to_lbj'  : ('*', 0.22480894)
    , 'kg.m/s2_to_N'    : ('==', 'kg.m/s2')
    , 'lbf_to_J/m'      : ('*', 4.44822160)
    , 'lbf_to_kg.m/s2'  : ('*', 4.44822160)
    , 'lbf_to_lb.ft/s2' : ('*', 32.174)
    , 'lb.ft/s2_to_lbf' : ('*', 0.031081)
    , 'lbf_to_N'        : ('*', 4.44822160)
    , 'N_to_lbf'        : ('*', 0.22480894)
    , 'N_to_J/m'        : ('==', 'N')
    , 'N_to_kg.m/s2'    : ('==', 'N')
    # lapse rate
    , 'dK/m_to_dC/km' : ('*', 1000.0)
    , 'dK/m_to_dF/km' : ('*', 1800.0)
    , 'dK/m_to_dF/ft' : ('*', 0.59055)
    , 'dK/m_to_dF/mi' : ('*', 3118.11)
    , 'dK/m_to_dK/km' : ('*', 1000.0)
    , 'dK/km_to_dC/m' : ('*', 0.001)
    , 'dK/km_to_dF/m' : ('*', 0.00055556)
    , 'dK/km_to_dK/m' : ('*', 0.001)
    # moisture content
    , 'kg/kg_to_g/kg'   : ('*', 1000.0)
    , 'g/kg_to_kg/kg'   : ('*', 0.001)
    # precipitation
    , 'cm_to_kg/m^2' : ('*', 10.0)
    , 'in_to_kg/m^2' : ('*', 25.4)
    , 'in_to_kg/m^2' : ('*', 25.4)
    , 'kg/m^2_to_in' : ('*', 0.039370)
    , 'kg/m^2_to_cm' : ('*', 0.1)
    , 'kg/m^2_to_mm' : ('==','x')
    , 'mm_to_kg/m^2' : ('==','x')
    # precipitation rate
    , 'cm/hr_to_kg/m^2/s' : ('*', 0.00277778)
    , 'in/hr_to_kg/m^2/s' : ('*', 0.00705555)
    , 'kg/m^2/s_to_cm/hr' : ('*', 360.0)
    , 'kg/m^2/s_to_in/hr' : ('*', 141.7323)
    , 'kg/m^2/s_to_mm/hr' : ('*', 3600.0)
    , 'mm/hr_to_kg/m^2/s' : ('*', 0.00027778)
    # solar radiation
    , 'watt/m^2_to_langley' : ('*', 0.086)
    , 'langley_to_watt/m^2' : ('*', 11.627907) # x / 0.086
    # speed - feet per second
    , 'ft/s_to_knt'  : ('*', 0.5924838)
    , 'ft/s_to_kph'  : ('*', 1.09728)
    , 'ft/s_to_m/s'  : ('*', 0.3048)
    , 'ft/s_to_mph'  : ('*', 0.681818)
    # speed - kilometers per hour
    , 'kph_to_ft/s'  : ('*', 0.911344)
    , 'kph_to_knt'   : ('*', 0.5399568)
    , 'kph_to_m/s'   : ('*', 0.277778)
    , 'kph_to_mph'   : ('*', 0.621317119)
    # speed - knots
    , 'knt_to_ft/s'  : ('*', 1.678099)
    , 'knt_to_kph'   : ('*', 1.852)
    , 'knt_to_m/s'   : ('*', 0.514444)
    , 'knt_to_mi/hr' : ('*', 1.1507794)
    , 'knt_to_mph'   : ('*', 1.1507794)
    # speed - miles per hour
    , 'mph_to_ft/s'  : ('*', 1.466666)
    , 'mph_to_knt'   : ('*', 0.86897624)
    , 'mph_to_kph'   : ('*', 1.609344)
    , 'mph_to_m/s'   : ('*', 0.44704)
    # speed - meters per second
    , 'm/s_to_ft/s'  : ('*', 3.28084)
    , 'm/s_to_knt'   : ('*', 1.943846)
    , 'm/s_to_kph'   : ('*', 3.6)
    , 'm/s_to_mph'   : ('*', 2.2369363)
    # temperature conversions
    , 'C_to_F'       : ('eq', '(x * 1.8) + 32.0')
    , 'C_to_K'       : ('eq', 'x + 273.15')
    , 'C_to_R'       : ('eq', '(x * 1.8) + 491.67')
    , 'F_to_C'       : ('eq', '(x - 32.) * 0.555556')
    , 'F_to_K'       : ('eq', '((x - 32.) * 0.555556) + 273.15')
    , 'F_to_R'       : ('eq', 'x + 459.67')
    , 'K_to_C'       : ('eq', 'x - 273.15')
    , 'K_to_F'       : ('eq', '((x - 273.15) * 1.8) + 32.0')
    , 'K_to_R'       : ('*', 1.8)
    , 'R_to_C'       : ('eq', '(x - 491.67) * 0.555556')
    , 'R_to_F'       : ('eq', 'x - 459.67')
    , 'R_to_K'       : ('*', 0.555556)
    # temperature - unit difference 
    , 'dC_to_dF'     : ('*', 1.8)
    , 'dC_to_dK'     : ('==','x')
    , 'dF_to_dC'     : ('*', 0.555556)
    , 'dF_to_dK'     : ('*', 0.555556)
    , 'dK_to_dC'     : ('==','x')
    , 'dK_to_dF'     : ('*', 1.8)
    # vertical velocity & pressure tendency
    , 'mb/s_to_ub/s'  : ('*', 1000.0)
    , 'mb/s_to_Pa/s'  : ('*', 100.0)
    , 'Pa/s_to_mb/hr' : ('*', 36.0)
    , 'Pa/s_to_ub/s'  : ('*', 10.0)
    , 'Pa/s_to_mb/s'  : ('*', 0.01)
    , 'ub/s_to_mb/s'  : ('*', 100.0)
    , 'ub/s_to_Pa/s'  : ('*', 0.1)
    # volume
    , 'ft^3_to_l'    : ('*', 28.316846)
    , 'ft^3_to_kl'   : ('*', 0.0283168)
    , 'ft^3_to_m^3'  : ('*', 0.0283168)
    , 'l_to_ft^3'    : ('*', 0.0353146)
    , 'kl_to_ft^3'   : ('*', 35.314666)
    , 'm^3_to_ft^3'  : ('*', 35.3147)
    # weight
    , 'g_to_kg'      : ('*', 0.001)
    , 'kg_to_g'      : ('*', 1000.0)
    , 'kg_to_lb'     : ('*', 2.2046226)
    , 'kg_to_N'      : ('*', 9.80665)
    , 'lb_to_kg'     : ('*', 0.4535924)
    , 'N_to_kg'      : ('*', 0.1019716)
    # unit are the same
    , 'equals'       : ('==','x')
    }

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# define the functions used to solve the conversions
def add(x, factor): return x + factor
def equals(x, factor): return x
def multiply(x, factor): return x * factor
def solve(x, equation): return eval(equation)
def subtract(x, factor): return x - factor
OPERATORS = { '+':add, '-':subtract, '*':multiply, '==':equals, 'eq':solve  }

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def conversionFormula(from_units, to_units):
    from_units = UNIT_KEY_MAP.get(from_units, from_units)
    to_units = UNIT_KEY_MAP.get(to_units, to_units)
    if from_units == to_units:
        return FORMULAS['equals']
    else:
        formula = FORMULAS.get('%s_to_%s' % (from_units,to_units), None)
        if formula is not None: return formula
        errmsg = 'No formula found to convert "%s" to "%s"'
        raise ValueError, errmsg % (from_units, to_units)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def conversionOperator(from_units, to_units):
    operation, arg = conversionFormula(from_units, to_units)
    return OPERATORS[operation], arg

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def isSupportedConversion(from_units, to_units):
    from_units = UNIT_KEY_MAP.get(from_units, from_units)
    to_units = UNIT_KEY_MAP.get(to_units, to_units)
    if from_units != to_units:
        formula = FORMULAS.get('%s_to_%s' % (from_units,to_units), None)
        if formula is None: return False
    return True

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def sanitizeUnits(units):
    # check for exponent
    if '**' in units: _units = units.replace('**','^')
    else: _units = units
    # check for scale
    if '*' in _units: return tuple(_units.split('*'))
    else: return (_units, None)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def convertUnits(data, data_units, out_units):
    if data_units == out_units: return data

    # check for scaled unit conversions
    from_units, from_scale = sanitizeUnits(data_units)
    to_units, to_scale = sanitizeUnits(out_units)

    is_array = isinstance(data, N.ndarray)
    if from_scale is not None:
        if is_array:
            if data.dtype.kind == 'f': data /= float(from_scale)
            elif data.dtype.kind == 'i': data /= int(from_scale)
        else:
            if type(data) == float: data /= float(from_scale)
            elif type(data) == int: data /= int(from_scale)

    from_units = UNIT_KEY_MAP.get(from_units, from_units)
    to_units = UNIT_KEY_MAP.get(to_units, to_units)
    if from_units != to_units:
        convert, arg = conversionOperator(from_units, to_units)
        result = convert(data, arg)

    if to_scale is not None:
        if is_array:
            if data.dtype.kind == 'f': result *= float(to_scale)
            elif data.dtype.kind == 'i': result *= int(to_scale)
        else:
            if type(data) == float: result *= float(from_scale)
            elif type(data) == int: result *= int(from_scale)
    return result

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def conversionFunction(from_units, to_units):
    if from_units is not None and to_units is not None:
        def convert(data): return convertUnits(data, from_units, to_units)
        return convert
    return None

