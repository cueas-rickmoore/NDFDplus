
import re

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

RE_FLOAT = re.compile(r"^'[+-]?(([0-9]+\.{1}[0-9]*)|(\.{1}[0-9]+))([eE][+-]?[0-9]+)?'$")
def isFloat(chars):
    if RE_FLOAT.match(chars): return True
    return False

RE_INTEGER = re.compile(r"^[+-]\d+$")
def isInteger(chars):
    if RE_INTEGER.match(chars): return True
    return False

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def tupleFromString(_string_, as_type=None):
    if _string_.startswith('['):
        right = _string_.rfind(']')
        _str_ = _string_[1:right].strip()
        it = [substr.strip() for substr in _str_.split(',')]
    elif _string_.startswith('('):
        right = _string_.rfind(')')
        _str_ = _string_[1:right].strip()
        it = [substr.strip() for substr in _str_.split(',')]
    else: it = [substr.strip() for substr in _string_.split(',')]
    if as_type is not None:
        return tuple([as_type(item) for item in it])
    else: return tuple(it)

def strippedFloat(float_num, precision=5, format='%%20.%df'):
    return ((format % precision) % float_num).strip()

