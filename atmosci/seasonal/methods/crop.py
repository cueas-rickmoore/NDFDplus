
import os
import datetime

from atmosci.utils.config import ConfigObject

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# CONVENIENCE FUNCTIONS COMMON TO PROJECTS WITH CROP VARIETY FILES
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class CropVarietyPathMethods:
    """ Common crop variety filepath mathods.

    DEPENDENCY: 
        from atmosci.seasonal.methods.paths import PathConstructionMethods
    
    designed to be included in a subclass derived from 
        atmosci.seasonal.factory.BaseProjectFactory

    """

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def templateArgs(self, target_year=None, source=None, region=None,
                           variety=None):
        template_args =  { }
        if target_year is not None: template_args['year'] = target_year
        if source is not None:
             template_args['source'] = self.sourceToFilepath(source)
        if region is not None:
            template_args['region'] = self.regionToFilepath(region)
        if variety is not None:
            template_args['variety'] = self.varietyToFilepath(variety)
        return template_args

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def varietyDirpath(self, variety, target_year, source, region):
        dirpath = self.sourceDirpath(source, region)
        if target_year is not None:
            dirpath = os.path.join(dirpath, str(target_year))
        dirpath = os.path.join(dirpath, self.varietyToDirpath(variety))
        self._verifyDirpath(dirpath)
        return dirpath

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def varietyFilename(self, variety, target_year, source, region, **kwargs):
        template = self.filenameTemplate('variety')
        template_args = self.templateArgs(target_year, source, region, variety) 
        template_args.update(dict(kwargs))
        return template % template_args

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def varietyFilepath(self, variety, target_year, source, region, **kwargs):
        dirpath = self.varietyDirpath(variety, target_year, source, region)
        filename = self.varietyFilename(variety, target_year, source,
                                        region, **kwargs)
        return os.path.join(dirpath, filename)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def varietyName(self, variety):
        if isinstance(variety, ConfigObject):
            tag = variety.get('tag', None)
            if tag is not None: return tag
            else: return variety.name
        elif isinstance(variety, basestring): return variety
        elif isinstance(variety, dict): return variety['name']
        else:
            raise TypeError, 'Unsupported type for "variety" argument.'

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def varietyToDirpath(self, variety):
        return self.normalizeDirpath(self.varietyName(variety).lower())

    def varietyToFilepath(self, variety):
        return self.normalizeFilepath(self.varietyName(variety))

