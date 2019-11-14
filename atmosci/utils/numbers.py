""" Mathematical algorithms and utility functions.
"""

import numpy as N

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def compare(value1, value2, precision=3):
    i_value1 = round(value1, precision) * 10**precision
    i_value2 = round(value2, precision) * 10**precision
    if i_value1 > i_value2:
        return 1
    if i_value1 < i_value2:
        return -1
    return 0

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def _fixArrayPrecision(values, *args, **kwargs):
    decimals = kwargs.get('decimals',2)
    precision = 10**decimals
    int_values = N.array(values,dtype=int)
    decimal_part = N.array(((values-int_values) * precision),dtype=int) 
    return int_values + (decimal_part / float(precision))

def fixPrecision(value, decimals=2):
    if isinstance(value, N.ndarray):
        return _fixArrayPrecision(value, N.where(N.isfinite(value)),
                                  decimals=decimals)
    # scalar value
    if not N.isfinite(value): return value
    precision = 10**decimals
    return int(value) + (int((value-int(value)) * precision) / float(precision))

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def roundUp(value):
    if isinstance(value, N.ndarray):
        value[value > 0.] += 0.5
        value[value < 0.] -= 0.4
        return N.array(N.array(value,dtype=int),dtype=float)
    else:
        if value >= 0.0:
            return float(int(value + 0.5))
        else:
            return float(int(value - 0.4))

