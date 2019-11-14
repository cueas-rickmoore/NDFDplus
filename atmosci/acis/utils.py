
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def elementID(element):
    if isinstance(element,  basestring):
        if element.isdigit():
            return int(element)
        else:
            return element
    elif type(element) == int:
        return element
    elif isinstance(element,  dict):
        if 'vX' in element:
            return int(element['vX'])
        else:
            return element['name']

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def indexableElementID(element):
    if isinstance(element,  basestring):
        if element.isdigit():
            return INDEXABLE_ELEMENT_IDS(element,'vX_%s' % element)
        else:
            return element
    elif type(element) == int:
        return INDEXABLE_ELEMENT_IDS(element,'vX_%d' % element)
    elif isinstance(element,  dict):
        if 'vX' in element:
            return INDEXABLE_ELEMENT_IDS(element['vX'],'vX_%d' % element['vX'])
        else:
            return element['name']

    errmsg = 'Invalid element identification, %s is an unsupported type'
    raise ValueError, errmsg % type(element)

