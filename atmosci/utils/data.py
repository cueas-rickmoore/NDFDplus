
import numpy as N

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class AsciiSafeDict(dict):

    @classmethod
    def makeSafe(_class_, _dict, safe_values=False):
        _safedict = AsciiSafeDict()
        if safe_values:
            for key, value in _dict.items():
                _safedict[safeDataKey(key)] = safevalue(value)
        else:
            for key, value in _dict.items():
                _safedict[safeDataKey(key)] = value
        return _safedict

def safestring(_string):
    if isinstance(_string, basestring):
        return _string.encode('iso-8859-1')
    return _string

def safeDataKey(key):
    if isinstance(key, basestring):
        return key.encode('iso-8859-1')
    errmsg = 'Dataset names must be strings, %s is not allowed : %s'
    raise TypeError, errmsg % (type(key), key)

def safevalue(value):
    if isinstance(value,N.ndarray):
        return tuple([safevalue(v) for v in list(value)])

    elif hasattr(value,'dtype'):
        if value.dtype.kind == 'S': return str(value)
        elif value.dtype.kind == 'f': return float(value)
        elif value.dtype.kind == 'i': return int(value)

    elif isinstance(value, unicode):
        if value.isdigit(): return int(value)
        try: return float(value)
        except ValueError: pass
        return value.encode('iso-8859-1')

    elif isinstance(value, basestring):
        if value.isdigit(): return int(value)
        try: return float(value)
        except ValueError: pass

    return value

def safedict(_dict, safe_values=False):
    return AsciiSafeDict.makeSafe(_dict, safe_values)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

DATA_FORMATS = { } 
DATA_FORMATS['elev'] = '%7.1'
DATA_FORMATS['lat'] = '%9.5f'
DATA_FORMATS['lon'] = '%10.5f'

def getDataFormat(dataset_name, data=None):
    format = DATA_FORMATS.get(dataset_name,None)
    if format is not None: return format

    if data is not None: # make a guess based on data type
        if isinstance(data, int) or (hasattr(data, 'dtype') and
                                     data.dtype.kind == 'i'):
            return '%d'

        elif isinstance(data, float):
            data_str = str(data)
            decimal_pt = data_str.find('.')
            right_side = max( ((len(data_str) - decimal_pt) - 1), 5)
            left_side = decimal_pt + 1 # need num chars, not index
            return '%d.%df' % (left_side+right_side, right_side)

        elif hasattr(data, 'dtype') and data.dtype.kind == 'f':
            valid = data[N.where(N.isfinite(data))]
            data_max = N.max(valid)
            max_str = str(data_max)
            decimal_pt = max_str.find('.')
            right_side = max( ((len(max_str) - decimal_pt) - 1), 5)

            left_side = decimal_pt + 1 # need num chars, not index
            if N.min(valid) < 0: left_side += 1 # add 1 for sign
            return '%d.%df' % (left_side+right_side, right_side)

    return "'%s'"

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def stringToBbox(bbox):
    errmsg = 'Invalid bbox string : %s' % bbox
    if '(' in bbox or '[' in bbox:
        return tuple([float(coord) for coord in eval(bbox)])
    elif ',' in bbox:
        values = bbox.split(',')
        if len(values) == 4:
            return tuple([float(coord) for coord in values])
        elif len(values) > 4:
            return tuple([float(coord) for coord in values[:4]])
        else:
            raise ValueError, errmsg
    else:
        raise ValueError, errmsg

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def dictToConstraints(_dict, separator=') & ('):
    if _dict is None: return None

    constraints = [ ]
    for key, constraint in _dict.items():
        if constraint is None: continue

        if key == 'bbox':
            if isinstance(constraint, basestring):
                bbox = stringToBbox(constraint)
            else: bbox = constraint

            data_format = getDataFormat('lon')
            constraints.append('lon >= ' + data_format % bbox[0])
            constraints.append('lon <= ' + data_format % bbox[2])

            data_format = getDataFormat('lat')
            constraints.append('lat >= ' + data_format % bbox[1])
            constraints.append('lat <= ' + data_format % bbox[3])
            continue

        if isinstance(constraint, basestring):
           if constraint in ('N.isnan','N.isinf','N.isfinite'):
               constraints.append('%s(%s)' % (constraint, key))
           else: constraints.append("%s == '%s'" %(key, constraint))

        elif isinstance(constraint, (tuple,list)):
            if len(constraint) == 1:
                if constraint[0] in ('N.isnan','N.isinf','N.isfinite'):
                    constraints.append('%s(%s)' % (constraint[0], key))
                else:
                    if isinstance(constraint[0], basestring):
                        constraints.append("%s == '%s'" %(key, constraint))
                    else:
                        data_format = getDataFormat(key, constraint[0])
                        constraints.append(key + ' == ' +
                                           data_format % constraint[0])
            elif len(constraint) == 2:
                data_format = getDataFormat(key, constraint[1])
                constraint_format = key + ' %s ' + data_format
                constraints.append(constraint_format % constraint)
            elif len(constraint) == 3:
                constraint_format = key + ' %s ' + constraint[2]
                constraints.append(constraint_format % constraint[:2])

        else:
            constraints.append("%s == %s" %(key, str(constraint)))

    if len(constraints) > 1:
        return '('+ separator.join(constraints) + ')'
    elif len(constraints) == 1:
        return constraints[0]
    else:
        return None

def dictToWhere(_dict):
    constraints = dictToConstraints(_dict, ') & (')
    if constraints is None: return None
    return 'N.where(%s)' % constraints

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def listToConstraints(_list_or_tuple, separator=') & ('):
    constraints = [ ]

    for constraint in _list_or_tuple:
        if constraint[0] == 'bbox':
            bbox = constraint[1]
            data_format = getDataFormat('lon')
            constraints.append('lon >= ' + data_format % bbox[0])
            constraints.append('lon <= ' + data_format % bbox[2])
            data_format = getDataFormat('lat')
            constraints.append('lat >= ' + data_format % bbox[1])
            constraints.append('lat <= ' + data_format % bbox[3])
        else:
            if len(constraint) == 2:
                if constraint[1] in ('N.isnan','N.isinf','N.isfinite'):
                    constraints.append('%s(%s)' % (constraint[1], constraint[0]))
                else:
                    data_format = getDataFormat(constraint[0], constraint[1])
                    constraints.append(constraint[0] + ' == ' +
                                       data_format % constraint[1])
            elif len(constraint) == 3:
                data_format = getDataFormat(constraint[0], constraint[2])
                constraint_format = '%s %s ' + data_format
                constraints.append(constraint_format % constraint)
            elif len(constraint) == 4:
                constraint_format = '%s %s ' + constraint[3]
                constraints.append(constraint_format % constraint[:3])

    if len(constraints) > 1:
        return '(' + separator.join(constraints) + ')'
    elif len(constraints) == 1:
        return constraints[0]
    else:
        return None

def listToWhere(list_or_tuple):
    constraints = listToConstraints(list_or_tuple, ') & (')
    if constraints is None: return None
    return 'N.where(%s)' % constraints

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def replaceInvalid(numpy_array, replace_with, missing_value=N.nan):
    numpy_array = N.where(N.isfinite(numpy_array),numpy_array,replace_with)
    if N.isfinite(missing_value):
        numpy_array[N.where(numpy_array==missing_value)] = replace_with
    return numpy_array

def validValues(numpy_array, missing_value=N.nan):
    if N.isfinite(missing_value):
        indexes = N.where(N.isfinite(numpy_array) & (numpy_array!=missing_value))
    else:
        indexes = N.where(N.isfinite(numpy_array))

    if isinstance(indexes, list):
        if len(indexes) == len(numpy_array): return numpy_array
        elif len(indexes) == 0: return None
        else: return numpy_array[indexes]
    else:
        if len(indexes[0]) == len(numpy_array): return numpy_array
        elif len(indexes[0]) == 0: return None
        else: return numpy_array[indexes[0]]

