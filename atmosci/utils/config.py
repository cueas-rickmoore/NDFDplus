
from collections import OrderedDict
from copy import deepcopy

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

BOGUS_VALUE = "~!@#$%^&*()-+=|}{:;<>?"
GETATTR_FAILED = hash('attribute/object path lookup failed')
RESERVED = ('__ATTRIBUTES__', '__CHILDREN__', '__RESERVED__', 
            'name', 'parent', 'proper_name')


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def configToJson(config_obj, is_child=False):
    import json
    if is_child: obj_json = '"%s":' % config_obj.name
    else: obj_json = '{"%s":' % config_obj.name

    if config_obj.has_attrbutes:
        obj_json = \
            '%s%s' % (obj_json,json.dumps(dict(config_obj.attritems())))
        obj_has_attrs = True
    else: obj_has_attrs = False

    children = [ ]
    if config_obj.has_children:
        if obj_has_attrs: obj_json = obj_json[:-1] + ','
        else: obj_json += '{'
        for name, child in config_obj.items():
            children.append(configToJson(child, True))
        obj_json = '%s%s}' % (obj_json, ','.join(children))

    if is_child: return obj_json
    return obj_json.replace(': ',':').replace(', ',',') + '}'


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def dictToConfig(dict_, name='config', parent=None):
    config = ConfigObject(name, parent)
    for key, value in dict_.items(): config[key] = value
    return config

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class ConfigIterator(object):

    def __init__(self, config, iteratable='keys'):
        self.current = 0
        self.num_attrs = len(config.__dict__['__ATTRIBUTES__'])
        self.num_children = len(config.__dict__['__CHILDREN__'])
        self.total_items = self.num_children + self.num_attrs

        if iteratable == 'items':
            self.iter_attributes = config.__dict__['__ATTRIBUTES__'].iteritems()
            self.iter_children = config.__dict__['__CHILDREN__'].iteritems()
        elif iteratable == 'values':
            self.iter_attributes = config.__dict__['__ATTRIBUTES__'].itervalues()
            self.iter_children = config.__dict__['__CHILDREN__'].itervalues()
        else:
            self.iter_attributes = config.__dict__['__ATTRIBUTES__'].iterkeys()
            self.iter_children = config.__dict__['__CHILDREN__'].iterkeys()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def __iter__(self):
        return self

    def next(self):
        try:
            return self.iter_children.next()
        except StopIteration:
            return self.iter_attributes.next()


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class ConfigMap(dict):

    def __init__(self, mapping_dict):
        for name, value in mapping_dict.items():
            self[name] = value


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class ConfigObject(object):

    def __init__(self, name, parent, *children, **kwargs):
        if isinstance(name, basestring) and '.' in name:
            errmsg = 'Trying to create instance of ConfigObject with name = "%s"'
            errmsg += '\nDotted paths cannot be used as a name for ConfigObjects'
            raise ValueError, errmsg % name

        object.__init__(self)
        self.__dict__['isOrdered'] = False
        self.__dict__['__ATTRIBUTES__'] = { }
        self.__dict__['__CHILDREN__'] = { }
        self.__dict__['__RESERVED__'] = RESERVED

        self._set_name_(name)

        if len(children) > 0:
            for child in children:
                if isinstance(child, basestring):
                    self.newChild(child)
                elif isinstance(child, ConfigObject):
                    self.addChild(child)
                else:
                    indx = children.index(child)
                    raise TypeError, 'Invalid type for argument %d' % indx

        if len(kwargs) > 0:
            for key, value in kwargs.items():
                self.__dict__['__ATTRIBUTES__'][key] = value

        if parent is not None: parent.addChild(self)
        else: self.__dict__['parent'] = None

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _path(self):
        if self.parent is None: return self.name
        else: return '%s.%s' % (self.parent._path(), self.name)
    path = property(_path)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    #
    # manage the dictionary of child objects
    #
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def addChild(self, config_obj):
        if config_obj.name in self.__dict__['__CHILDREN__']:
            del self.__dict__['__CHILDREN__'][config_obj.name]
        self.__dict__['__CHILDREN__'][config_obj.name] = config_obj
        config_obj.__dict__['parent'] = self

    def addChildren(self, *children):
        for item in children:
            if isinstance(item, ConfigObject):
                self.addChild(item)
            elif isinstance(item, basestring):
                self.__dict__['__CHILDREN__'][item] = ConfigObject(item, self)
            else:
                errmsg = '"%s" is an invalid type for a child of ConfigObject,'
                TypeError, errmsg % type(item)

    def asDict(self):
        me = { 'name':self.name, }
        if self.parent is not None:
            me['parent'] = self.parent.name
        else: me['parent'] = None
        if self.hasAttributes():
            me['attributes'] = dict(self.attritems())
        for name, child in self.items():
            me[name] = child.asDict()
        return me
    dict = property(asDict)

    def clear(self):
        self.__dict__['__CHILDREN__'].clear()

    def copy(self, new_name=None, parent=None):
        if new_name is None: name = self.name
        else: name = new_name
        _copy = self._complete_copy_(self.__class__(name, None))
        if parent is not None: # copying entire config tree
            parent.addChild(_copy)
        return _copy

    def extend(self, obj):
        if isinstance(obj, ConfigObject):
            if obj.name in self.child_names: self.update(obj)
            else: self.addChild(obj)
        elif isinstance(obj, dict):
            for key, value in obj.items():
                self._set_value_of_([key,], value)
        else:
            raise TypeError, "Invalid type for 'obj' argument : %s" % type(obj)

    def find(self, path, default=BOGUS_VALUE):
        _path = self._path_to_list_(path)
        # check to see if my own name is at the beginning of the path
        if self._is_my_name_(_path[0]):
            return self._find_(_path, 1, default)
        return self._find_(_path, 0, default)

    def flatten(self):
        flat = { }
        if self.hasAttributes():
            flat.update(dict(self.attritems()))
        for name, child in self.items():
            flat[name] = child.flatten()
        return flat

    def hierarchy(self, seed=None, hierarchy=None):
        my_name = self.__dict__['name']
        if seed is None: _seed = my_name
        else: _seed = '%s.%s' % (seed, my_name)
        if hierarchy is None: hierarchy = [ _seed, ]
        else: hierarchy.append(_seed)

        for name, child in self.__dict__['__CHILDREN__'].items():
            hierarchy = child.hierarchy(_seed, hierarchy)

        if seed is None: return tuple(hierarchy)
        else: return hierarchy

    def inheritAttrs(self, obj):
        my_attrs = self.__dict__['__ATTRIBUTES__'].keys()
        if isinstance(obj, ConfigObject):
            for key, value in obj.__dict__['__ATTRIBUTES__'].items():
                if key not in my_attrs:
                    self._set_value_of_([key,], value)
        elif isinstance(obj, dict):
            for key, value in obj.items():
                if key not in my_attrs:
                    self._set_value_of_([key,], value)
        else:
            raise TypeError, "Invalid type for 'obj' argument : %s" % type(obj)

    def link(self, config_obj, path=None, alias=None):
        if alias is None: link_name = config_obj.name
        else: link_name = alias
        if path is None:
            self.__dict__['__CHILDREN__'][link_name] = config_obj
        else:
            link_to = self._get_value_of_(self._path_to_list_(path), None)
            if link_to is None:
                errmsg = 'Invalid path "%s", onject not in this config.'
                raise LookupError, errmsg % path
            else:
                link_to.__dict__['__CHILDREN__'][link_name] = config.obj

    def newChild(self, path, obj=None):
        if '.' not in path:
            if path not in self.__dict__['__CHILDREN__']:
                if obj is None:
                    child = ConfigObject(path, self)
                    self.__dict__['__CHILDREN__'][path] = child
                else: self._ingest_(self, path, obj, must_be_object=True)
            else:
                if path in self.__dict__['__ATTRIBUTES__']:
                    raise KeyError, 'Attribute aleady exists for "%s"' % path
                else: raise KeyError, 'Child alrady exists at "%s"' % path
        else:
            _path = self._path_to_list_(path)
            if obj is None:
                child = self._construct_obj_tree_(_path)
            else:
                if self._is_convertable_(obj):
                    child = self._construct_obj_tree_(_path[:-1])
                    child._ingest_(path[-1], obj, must_be_object=True)
                else:
                    errmsg = 'Value for "%s" is cannot be converted to an object.' 
                    raise TypeError, errmsg % path
        return child

    def update(self, obj):
        if isinstance(obj, ConfigObject):
            if obj.name == self.name: self._update_(obj)
            elif obj.name in self.child_names:
                self[obj.name].update(obj)
            else: self.addChild(obj)
        elif isinstance(obj, dict):
            for key, value in obj.items():
                self._set_value_of_([key,], value)
        else:
            raise TypeError, "Invalid type for 'obj' argument : %s" % type(obj)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # standard iterators are iterators over children
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def items(self): return self.__dict__['__CHILDREN__'].items()
    def iteritems(self): return self.__dict__['__CHILDREN__'].iteritems()

    def keys(self): return self.__dict__['__CHILDREN__'].keys()
    def iterkeys(self): return self.__dict__['__CHILDREN__'].iterkeys()

    def values(self): return self.__dict__['__CHILDREN__'].values()
    def itervalues(self): return self.__dict__['__CHILDREN__'].itervalues()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # child retrieval and tests
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def childDict(self):
        return dict(self.__dict__['__CHILDREN__'].items())

    def childList(self):
        return list(self.__dict__['__CHILDREN__'].values())

    def childNames(self):
        return tuple(self.__dict__['__CHILDREN__'].keys())
    child_names = property(childNames)

    def _children_(self):
        return tuple(self.__dict__['__CHILDREN__'].values())
    children = property(_children_)

    def hasChildren(self):
        return len(self.__dict__['__CHILDREN__']) > 0
    has_children = property(hasChildren)

    def isChild(self, name):
        return name in self.__dict__['__CHILDREN__'].keys()
    has_child = isChild

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # iterators over attributes
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def attritems(self):
        for name in self.attrNames():
            yield (name, self.__dict__['__ATTRIBUTES__'][name])
    attr_items = property(attritems)

    def attrkeys(self):
        for name in self.attrNames(): yield name
    attr_keys = property(attrkeys)

    def attrvalues(self):
        for name in self.attrNames():
            yield self.__dict__['__ATTRIBUTES__'][name]
    attr_values = property(attrvalues)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # attribute retrieval and tests
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def attrDict(self):
        return dict(self.__dict__['__ATTRIBUTES__'].items())
    attrs = property(attrDict)
    attributes = property(attrDict)

    def attrList(self):
        values = [ self.__dict__['__ATTRIBUTES__']['name']
                   for name in self.attrNames() ]
        return tuple(values)
    attr_list = property(attrList)

    def attrNames(self):
        names = self.__dict__['__ATTRIBUTES__'].keys()
        names.sort()
        return tuple(names)
    attr_names = property(attrNames)

    def hasAttributes(self):
        return len(self.__dict__['__ATTRIBUTES__']) > 0
    has_attrbutes = property(hasAttributes)

    def hasAttribute(self, name):
        return name in self.__dict__['__ATTRIBUTES__'].keys()
    isAttribute = hasAttribute # legacy compatability

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # tests work on children and attributes
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def all_keys(self):
        keys = ( self.__dict__['__CHILDREN__'].keys() +
                 self.__dict__['__ATTRIBUTES__'].keys() )
        return keys

    def has_key(self, key):
        return ( key in self.__dict__['__CHILDREN__'].keys() or
                 key in self.__dict__['__ATTRIBUTES__'].keys() )

    def isReservedKey(self, key):
        return key in self.__dict__['__RESERVED__']

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    #
    # standard value getter/setter ... work for both attributes abd children
    #
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def get(self, path, default=None):
        return self._get_value_of_(self._path_to_list_(path), default)

    def set(self, **kwargs):
        for path, _value in kwargs.items():
            self._set_value_of_(self._path_to_list_(path), _value)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    #
    # ConfigObject "dirty work" methods
    #
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _complete_copy_(self, _copy):
        obj_dict_keys = _copy.__dict__.keys()

        for key, config_obj in self.__dict__['__CHILDREN__'].items():
            _copy.__dict__['__CHILDREN__'][key] = config_obj.copy(key, _copy)

        for key, value in self.__dict__['__ATTRIBUTES__'].items():
            if isinstance(value, tuple):
                #_copy.__dict__['__ATTRIBUTES__'][key] = tuple([x for x in value])
                _copy.__dict__['__ATTRIBUTES__'][key] = deepcopy(value)
            elif isinstance(value, list):
                #_copy.__dict__['__ATTRIBUTES__'][key] = [x for x in value]
                _copy.__dict__['__ATTRIBUTES__'][key] = deepcopy(value)
            elif isinstance(value, OrderedDict):
                _copy.__dict__['__ATTRIBUTES__'][key] =\
                      OrderedDict(deepcopy(tuple(OrderedDict.items())))
            elif isinstance(value, ConfigObject):
                _copy.__dict__['__ATTRIBUTES__'][key] = value.copy(parent=self)
            else: _copy.__dict__['__ATTRIBUTES__'][key] = deepcopy(value)

        for key in self.__dict__.keys():
            if key not in obj_dict_keys:
                _copy.__dict__[key] = deepcopy(self.__dict__[key])

        return _copy

    def _construct_obj_tree_(self, keys):
        name = keys[0]
        child = self.__dict__['__CHILDREN__'].get(name, None)
        if child is None: child = self.newChild(name)
        if len(keys) > 1 : child._construct_obj_tree_(keys[1:]) 

    def _delete_child_(self, name):
        child = self.__dict__['__CHILDREN__'].get(name, None)
        if child is not None:
            child._delete_children_()
            del self.__dict__['__CHILDREN__'][name]

    def _delete_children_(self):
        for key, child in self.__dict__['__CHILDREN__'].items():
            child._delete_children_()
            del self.__dict__['__CHILDREN__'][key]

    def _delete_tree_(self, path):
        child = self.__dict__['__CHILDREN__'].get(path[0], None)
        if child is not None:
            if len(path) > 1:
                child._delete_tree_(path[1:])
            del self.__dict__['__CHILDREN__'][path[0]]

    def _dict_keys_(): return self.__dict__.keys()
    dict_keys = property(_dict_keys_)

    def _find_(self, path, depth=0, default=BOGUS_VALUE):
        if path[-1] == '*':
            errmsg = 'Path may not end with an asterisk : "%s"'
            raise KeyError, errmsg % '.'.join(path)

        path_len = len(path)
        plus_1 = depth + 1

        key = path[depth]
        if path_len == plus_1:
            if key == 'attributes': self.attrDict()
            elif key == 'children': return self.children()
            elif key == 'dict': return self.asDict()
            elif key == 'self': return self

        if key != '*':
            child = self.__dict__['__CHILDREN__'].get(key, None)
            if child is not None:
                if path_len == plus_1: return child
                else: 
                    _found = child._find_(path, plus_1, default)
                    if _found is not None: return _found
                    if default != BOGUS_VALUE: return default
            else:
                if path_len == plus_1:
                    _found = self.__dict__['__ATTRIBUTES__'].get(key, None)
                    if _found is not None: return _found
                if default != BOGUS_VALUE: return default
                errmsg = 'Path ends prematurely at "%s"'
                raise KeyError, errmsg % '.'.join(path[:plus_1])

        else: # asterisk is never at the end of the path
            found = [ ]
            # asterisk in path, loop through all children
            # return all matches after asterisk
            for child in self.values():
                _found = child._find_(path, plus_1, None)
                if _found is not None:
                    found_path = '.'.join(path).replace('*',child.name)
                    found.append((found_path, _found))
            if found: return dict(found)
            if default != BOGUS_VALUE: return default

        # will only get here at the top level of the path stack
        errmsg = '"%s" does not correspond to any object'
        raise KeyError, errmsg % '.'.join(path)

    def _get_value_of_(self, path, default=GETATTR_FAILED):
        path_len = len(path)
        key = path[0]

        if self.parent is None and self._is_my_name_(key):
            if path_len == 1: return self
            else: return self._get_value_of_(path[1:], default)

        child = self.__dict__['__CHILDREN__'].get(key, None)
        if child is not None:
            if path_len == 1: return child
            else: return child._get_value_of_(path[1:], default)

        if path_len == 1:
            value = self.__dict__['__ATTRIBUTES__'].get(key, None)
            if value is not None: return value
            elif key == 'dict': return self.asDict()
            elif key == 'self': return self
            else: return self.__dict__.get(key, default)

        return default

    def _ingest_(self, key, value, must_be_object=False):
        if key in self.__dict__['__RESERVED__']:
            raise KeyError, 'Path contains a reserved name key "%s"' % key
        if self.has_key(key): self._delete_child_(key)

        if isinstance(value, OrderedDict):
            child = OrderedConfigObject(key, self)
            for _key, _value in value.items():
                child.__dict__['__ATTRIBUTES__'][_key] = _value
            self.addChild(child)
        elif isinstance(value, ConfigMap):
            self.__dict__['__ATTRIBUTES__'][key] = value
        elif isinstance(value, dict):
            child = ConfigObject(key, None)
            for _key, _value in value.items():
                child._ingest_(_key, _value)
            self.addChild(child)
        elif isinstance(value, ConfigObject):
            self.__dict__['__CHILDREN__'][key] = value.copy(parent=self)
        else:
            if must_be_object:
                raise TypeError, 'Value of "%s" is not an object.' % key
            self.__dict__['__ATTRIBUTES__'][key] = value

    def _is_convertable_(self, obj):
        return isinstance(obj, (ConfigObject, OrderedDict, dict))

    def _is_my_name_(self, name):
        return name == self.name

    def _path_to_list_(self, path):
        if isinstance(path, basestring): return path.split('.')
        elif isinstance(path, (tuple,list)): return path
        elif isinstance(path, int): return [path,]
        else: 
            errmsg = '%s is an invalid type for "path" argument.'
            raise TypeError, errmsg % type(path)

    def _proper_name_(self, name):
        if isinstance(name, basestring):
            return name.replace('_',' ').title()
        else: return str(name)

    def _set_name_(self, new_name):
        self.__dict__['name'] = new_name
        self.__dict__['proper_name'] = self._proper_name_(new_name)

    def _set_value_of_(self, path, value, depth=0):
        path_len = len(path)
        plus_1 = depth + 1

        if path_len == plus_1:
            self._ingest_(path[-1], value)
        else:
            child = self.__dict__['__CHILDREN__'].get(path[depth], None)
            if child is not None:
                child._set_value_of_(path, value, depth=plus_1)
            else:
                child = self.newChild(path[depth])
                child._set_value_of_(path, value, depth=plus_1)

    def _top_(self):
        if self.parent is None: return self
        else: return self.parent._top_()

    def _update_(self, obj):
        for key, child in obj.__dict__['__CHILDREN__'].items():
            if key in self.__dict__['__CHILDREN__']:
                self.__dict__['__CHILDREN__'][key]._update_(child)
            #elif isinstance(child, ConfigObject):
            #    self.__dict__['__CHILDREN__'][key] = child.copy(key, self)
            #else: self[key] = child
            else: self.__dict__['__CHILDREN__'][key] = child.copy(key, self)

        for key, value in obj.__dict__['__ATTRIBUTES__'].items():
            if isinstance(value, tuple):
                self.__dict__['__ATTRIBUTES__'][key] = tuple([x for x in value])
            elif isinstance(value, list):
                self.__dict__['__ATTRIBUTES__'][key] = [x for x in value]
            elif isinstance(value, OrderedDict):
                self.__dict__['__ATTRIBUTES__'][key] = OrderedDict(tuple(OrderedDict.items()))
            elif isinstance(value, ConfigObject):
                self.__dict__['__ATTRIBUTES__'][key] = value.copy(parent=self)
            else: self.__dict__['__ATTRIBUTES__'][key] = value

        for key in obj.__dict__.keys():
            if not self.isReservedKey(key):
                self.__dict__[key] = obj.__dict__[key]

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    #
    # overrides for builtin methods
    #
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def __contains__(self, key):
        contains = key in self.__dict__['__CHILDREN__'].keys() 
        return contains or key in self.__dict__['__ATTRIBUTES__'].keys()

    def __delitem__(self, path):
        self._delete_tree_(self._path_to_list_(path))

    def __getattr__(self, path):
        value = self._get_value_of_(self._path_to_list_(path), GETATTR_FAILED)
        try:
            if value != GETATTR_FAILED: return value
        except: # will get here if value is an 'object'
            return value
        raise KeyError, '"%s" is an invalid key' % path

    def __getitem__(self, path):
        value = self._get_value_of_(self._path_to_list_(path), None)
        if value is not None: return value
        raise KeyError, '"%s" is an invalid key' % path

    def __hash__(self):
        hash_key = list(self.keys())
        hash_key.insert(0, self.path)
        return hash(tuple(hash_key))

    def __iter__(self): return self.__dict__['__CHILDREN__'].iterkeys()

    def __len__(self):
        return len(self.__dict__['__CHILDREN__'])

    def __repr__(self):
        description = '<instance of class "%s" named "%s" with %s>'
        names = self.child_names
        if names: children = 'children "%s"' % '", "'.join(names)
        else: children = 'no children'
        return description % (self.__class__.__name__, self.name, children)

    def __set__(self, path, value):
        self._set_value_of_(self._path_to_list_(path), value)

    def __setitem__(self, path, value):
        self._set_value_of_(self._path_to_list_(path), value)

    def __setattr__(self, path, value):
        self._set_value_of_(self._path_to_list_(path), value)

    def __str__(self):
        return str(self.asDict())

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # comparison operators
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return hash(self) == hash(other)
        return False

    def __ge__(self, other):
        if isinstance(other, self.__class__):
            return hash(self) >= hash(other)
        return False

    def __gt__(self, other):
        if isinstance(other, self.__class__):
            return hash(self) > hash(other)
        return False

    def __le__(self, other):
        if isinstance(other, self.__class__):
            return hash(self) <= hash(other)
        return False

    def __lt__(self, other):
        if isinstance(other, self.__class__):
            return hash(self) < hash(other)
        return False

    def __ne__(self, other): return not self.__eq__(other)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class OrderedConfigObject(ConfigObject):

    def __init__(self, name, parent, *children, **kwargs):
        if '.' in name:
            errmsg = 'Trying to create instance of ConfigObject with name = "%s"'
            errmsg += '\nDotted paths cannot be used as a name for ConfigObjects'
            raise ValueError, errmsg % name

        object.__init__(self)
        self.__dict__['isOrdered'] = True
        self.__dict__['__ATTRIBUTES__'] = OrderedDict()
        self.__dict__['__CHILDREN__'] = OrderedDict()
        self.__dict__['__RESERVED__'] = RESERVED

        self._set_name_(name)

        if len(children) > 0:
            for child in children:
                if isinstance(child, basestring):
                    self.newChild(child)
                elif isinstance(child, ConfigObject):
                    self.addChild(child)
                else:
                    indx = children.index(child)
                    raise TypeError, 'Invalid type for argument %d' % indx

        if len(kwargs) > 0:
            for key, value in kwargs.items():
                self.__dict__['__ATTRIBUTES__'][key] = value

        if parent is not None: parent.addChild(self)
        else: self.__dict__['parent'] = None


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    #
    # manage the dictionary of child objects
    #
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def asDict(self):
        me = { 'name':self.name, }
        if self.parent is not None:
            me['parent'] = self.parent.name
        else: me['parent'] = None
        if self.has_children:
            me['children'] = OrderedDict(self.items())
        if self.hasAttributes():
            me['attributes'] = OrderedDict(self.attritems())
        return me
    dict = property(asDict)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # iterators over attributes
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def attritems(self):
        return self.__dict__['__ATTRIBUTES__'].iteritems()
    attr_items = property(attritems)

    def attrkeys(self):
        return self.__dict__['__ATTRIBUTES__'].iterkeys()
    attr_keys = property(attrkeys)

    def attrvalues(self):
        return self.__dict__['__ATTRIBUTES__'].itervalues()
    attr_values = property(attrvalues)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # attribute retrieval and tests
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def attrDict(self):
        return OrderedDict(self.__dict__['__ATTRIBUTES__'].items())
    attrs = property(attrDict)
    attributes = property(attrDict)

    def attrList(self):
        return list(self.__dict__['__ATTRIBUTES__'].values())
    attr_list = property(attrList)

    def attrNames(self):
        return tuple(self.__dict__['__ATTRIBUTES__'].keys())
    attr_names = property(attrNames)

