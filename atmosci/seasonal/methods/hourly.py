
import os
import datetime

from atmosci.seasonal.methos.factory import MinimalFactoryMethods
from atmosci.seasonal.methos.paths import PathConstructionMethods


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# CONVENIENCE FUNCTIONS COMMON TO MULTIPLE PROJECT TYPES
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class HourlyGridFactoryMethods(MinimalFactoryMethods,
                               PathConstructionMethods):
    """ Methods common to hourly grid factories.
    """
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def normalizeDirpath(self, obj_or_string):
        if isinstance(obj_or_string, basestring): name = obj_or_string
        else: name = obj_or_string.get('subdir', obj_or_string.name)
        _name = name.replace(' ','_').replace('-','_').replace('.',os.sep)
        return os.path.normpath(_name)

    def normalizeFilepath(self, obj_or_string):
        if isinstance(obj_or_string, basestring): name = obj_or_string
        else: name = obj_or_string.get('tag', obj_or_string.name)
        _name = name.replace('_',' ').replace('-',' ').replace('.',' ')
        return _name.title().replace(' ','-')

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def dateString(self, time_obj): return time_obj.strftime('%Y%m%d')
    def timeString(self, time_obj): return time_obj.strftime('%Y%m%d%H')

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def timeToDirpath(self, time_obj):
        if isinstance(time_obj, datetime.datetime):
            return self.timeString(time_obj)
        elif isinstance(time_obj, datetime.date):
            return self.dateString(time_obj)
        elif isinstance(time_obj, basestring):
            return time_obj
        else:
            errmsg = '"%s" is an invalid type for "timeobj" arg'
            raise TypeError, errmsg % type(time_obj)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def timeToFilepath(self, time_obj):
        if isinstance(time_obj, datetime.datetime):
            return self.timeString(time_obj)
        elif isinstance(time_obj, datetime.date):
            return self.dateString(time_obj)
        elif isinstance(time_obj, basestring):
            return time_obj
        else:
            errmsg = '"%s" is an invalid type for "timeobj" arg'
            raise TypeError, errmsg % type(time_obj)

del HourlyGridFactoryMethods.getDownloadFileTemplate
del HourlyGridFactoryMethods.projectGridFilename
del HourlyGridFactoryMethods.projectGridFilepath
del HourlyGridFactoryMethods.targetGridDir
del HourlyGridFactoryMethods.templateArgs
