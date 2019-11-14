
import os
import datetime

from atmosci.utils.config import ConfigObject

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# CONVENIENCE FUNCTIONS COMMON TO PROJECTS WITH CROP VARIETY FILES
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class SubProjectPathMethods:
    """ Common crop subproject filepath mathods.

    DEPENDENCY: 
        from atmosci.seasonal.methods.paths import PathConstructionMethods
    
    designed to be included in a subclass derived from 
        atmosci.seasonal.factory.BaseProjectFactory

    """

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def templateArgs(self, subproject, target_year=None, source=None,
                           region=None):
        template_args =  { }
        if target_year is not None: template_args['year'] = target_year
        if source is not None:
             template_args['source'] = self.sourceToFilepath(source)
        if region is not None:
            template_args['region'] = self.regionToFilepath(region)
        if subproject is not None:
            template_args['subproject'] = self.subprojectToFilepath(subproject)
        return template_args

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def subProjectDirpath(self, subproject, target_year, source, region):
        dirpath = self.sourceDirpath(source, region)
        if target_year is not None:
            dirpath = os.path.join(dirpath, str(target_year))
        dirpath = os.path.join(dirpath, self.subprojectToDirpath(subproject))
        self._verifyDirpath(dirpath)
        return dirpath

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def subProjectFilename(self, subproject, target_year, source, region,
                                 **kwargs):
        template = self.filenameTemplate('subproject')
        template_args = self.templateArgs(subproject, target_year, source,
                                          region) 
        template_args.update(dict(kwargs))
        return template % template_args

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def subProjectFilepath(self, subproject, target_year, source, region,
                                 **kwargs):
        dirpath = self.subProjectDirpath(subproject, target_year, source,
                                         region)
        filename = self.subProjectFilename(subproject, target_year, source,
                                           region, **kwargs)
        return os.path.join(dirpath, filename)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def subProjectName(self, subproject):
        if isinstance(subproject, ConfigObject):
            tag = subproject.get('tag', None)
            if tag is not None: return tag
            else: return subproject.name
        elif isinstance(subproject, basestring): return subproject
        elif isinstance(subproject, dict): return subproject['name']
        else:
            raise TypeError, 'Unsupported type for "subproject" argument.'

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def subProjectToDirpath(self, subproject):
        return self.normalizeDirpath(self.subProjectName(subproject).lower())

    def subProjectToFilepath(self, subproject):
        return self.normalizeFilepath(self.subProjectName(subproject))

