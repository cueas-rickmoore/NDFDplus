
import ast
import optparse

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def getBboxFromOptions(options):
    if hasattr(options, 'bbox') and options.bbox is not None:
        return stringToBbox(options.bbox)
    else:
        return None

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

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def optionsAsDict(options, trim=False):
    opt_dict = vars(options)
    if not trim: return opt_dict

    usable_options = { }
    for key,value in opt_dict.items():
        if value is None or (isinstance(value, (basestring, tuple, list, dict))
        and len(value) is 0): continue
        
        usable_options[key] = value
    return usable_options

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def typifyList(_list_, _type):
    for indx, item in enumerate(_list_):
        if isinstance(item, list): 
            _list_[indx] = typifyList(item, _type)
        else: _list_[indx] = _type(item)
    return _list_

def stringToList(_string, _type=None):
    _str = _string.strip()
    # replace parens with square brackets
    if '(' in _str: _str = _str.replace('(','[').replace(')',']')
    # string contains square brackets
    if '[' in _str:
        if _type is not None:
            return typifyList(ast.literal_eval(_str), _type)
        else: return ast.literal_eval(_str)

    # no square brackets or parens in string
    else:
        if _type is None:
            return [ s.strip() for s in _str.split(',') ]
        else: return [ _type(s) for s in _str.split(',') ]

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def listToTuples(_list_):
    for indx, item in enumerate(_list_):
        if isinstance(item, list): 
            _list_[indx] = listToTuples(item)
        # else ... no change to item
    return tuple(_list_)

def stringToTuple(_string, _type=None):
    _list = stringToList(_string, _type)
    return listToTuples(_list)

