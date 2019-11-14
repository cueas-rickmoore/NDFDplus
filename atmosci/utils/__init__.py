
import os, shutil

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class BogusValue:
    def __str__(self): return '!^BOGUS.VALUE^!'
BOGUS_VALUE = BogusValue()


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def backupFile(abs_filepath, backup_dirpath=None):
    if backup_dirpath is None:
        backup_filepath_ = abs_filepath
    else:
        dirpath, filename = os.path.split(abs_filepath)
        dirpath_ = os.path.normpath(backup_dirpath)
        backup_filepath_ = os.path.join(dirpath_, filename)

    backup_filepath = backup_filepath_ + '.backup'
    count = 0
    while os.path.exists(backup_filepath):
        count += 1
        backup_filepath = backup_filepath_ + '.backup%03d' % count
    shutil.copyfile(abs_filepath, backup_filepath)
    shutil.copymode(abs_filepath, backup_filepath)
    shutil.copystat(abs_filepath, backup_filepath)

    return backup_filepath


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class DotVariable(object):

    def __init__(self, *args, **kwargs):
        self.__variables__ = { }
        self.update(*args, **kwargs)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def clear(self):
        self.__variables__.clear()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def get(self, key, default=None):
        return self.__variables__.get(key, default)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def has_key(self, key):
        return key in self.__variables__

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def update(self, *args, **kwargs):
        num_args = len(args)
        if num_args == 1:
            if isinstance(args[0], dict):
                self.__variables__.update(args[0])
            elif isinstance(args[0], DotVariable):
                self.__variables__.update(args[0].items())
            else:
                errmsg = 'Unsupported type for args[0] : %s'
                raise TypeError, errmsg % str(type(args[0]))
        elif num_args == 2:
            if isinstance(args[0], (list,tuple)) \
            and isinstance(args[1], (list,tuple)):
                keys = tuple(args[0])
                values = tuple(args[1])
                self.__variables__.update(dict(zip(keys,values)))
        elif num_args > 2:
            errmsg = 'Invalid number of arguments : %d'
            raise ValueError, errmsg % len(args)

        if kwargs:
            self.__variables__.update(kwargs)


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # standard iterators
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def items(self): return self.__variables__.items()
    def iteritems(self): return self.__variables__.iteritems()

    def keys(self): return self.__variables__.keys()
    def iterkeys(self): return self.__variables__.iterkeys()

    def values(self): return self.__variables__.values()
    def itervalues(self): return self.__variables__.itervalues()


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    #
    # overrides for builtin methods
    #
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def __contains__(self, key):
        return key in self.__variables__.keys()

    def __delattr__(self, key):
        if key in self.__dict__: del self.__dict__[key]
        else: del self.__variables__[key]

    def __delitem__(self, key):
        del self.__variables__[key]

    def __getattr__(self, key):
        if key in self.__dict__: return self.__dict__[key]
        if key in self.__variables__: return self.__variables__[key]
        raise AttributeError, '"%s" is an invalid key' % key

    def __getitem__(self, key):
        if key in self.__variables__: return self.__variables__[key]
        raise KeyError, '"%s" is an invalid key' % key

    def __hash__(self):
        hash_key = list(self.keys())
        hash_key.insert(0, self.__class__.__name__)
        return hash(tuple(hash_key))

    def __iter__(self): return self.__variables__.iterkeys()

    def __len__(self):
        return len(self.__variables__)

    def __repr__(self):
        description = '<instance of class "%s" with %s>'
        names = self.__variables__.keys()
        if names: names = 'variables "%s"' % '", "'.join(names)
        else: names = 'no variables'
        return description % (self.__class__.__name__, names)

    def __set__(self, key, value):
        self.__variables__[key] = value

    def __setattr__(self, key, value):
        if key == '__variables__': self.__dict__[key] = value
        elif key in self.__dict__: self.__dict__[key] = value
        else: self.__variables__[key] = value

    def __setitem__(self, key, value):
        self.__variables__[key] = value

    def __str__(self):
        return str(self.__variables__.asDict())

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

