
import os

from atmosci.utils.config import ConfigObject

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class StaticFileAccessorMethods:
    """ Methods for generating static directory and file paths plus
    instantiating the correct file accessor for a specific source /
    region combination.

    REQUIRES:
        1. must be included in a class derived from 
           atmosci.seasonal.factory.BaseProjectFactory
           
        2. subclass must also inherit from
           atmosci.seasonal.methods.paths.PathConstructionMethods
           and
           atmosci.seasonal.methods.access.BasicFileAccessMethods

        3. subclasses must register 'read', 'write' and 'build' access
           managers for 'static' using:
              self._registerAccessManagers('static', ReaderClass,
                                           ManagerClass, BuilderClass)
              OR
              self._registerAccessManager('static', 'read', ReaderClass)
              self._registerAccessManager('static', 'manage', ManagerClass)
              self._registerAccessManager('static', 'build', BuilderClass)
    """

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # static file access
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def staticFileBuilder(self, source, region, **kwargs):
        filepath = kwargs.get('filepath',
                              self.staticGridFilepath(source,region,**kwargs))
        filetype = 'static.%s' % self.sourceName(source)
        return self.newProjectFileBuilder(filepath, filetype, source, None,
                                          region)
    getStaticFileBuilder = staticFileBuilder

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def staticFileManager(self, source, region, mode='r', **kwargs):
        filepath = kwargs.get('filepath',
                              self.staticGridFilepath(source,region,**kwargs))
        return self.newProjectFileAccessor(filepath, 'manage', 'static', mode)
    getStaticFileManager = staticFileManager

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def staticFileReader(self, source, region, **kwargs):
        filepath = kwargs.get('filepath',
                              self.staticGridFilepath(source,region,**kwargs))
        return self.newProjectFileAccessor(filepath, 'read', 'static')
    getStaticFileReader = staticFileReader

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # static source directory & file paths
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def staticFileTemplate(self, source):
        if isinstance(source, ConfigObject):
            # any config object might have a custom template
            template = self._findFilenameTemplate(source)
            if template is not None: return template
            # no custom template, a real source object should have a
            # corresponding static file configuration we can check
            if source.parent.name == 'sources':
                static_cfg = self.config.static.get(source.name, None)
                if static_cfg is not None:
                    template = self._findFilenameTemplate(static_cfg)
                    if template is not None: return template
            # when all else fails, use the object's name as the template key
            tmpl_key = source.name
        # assume source is a string that contains the template key
        else: tmpl_key = source
        template = self.config.filenames.get(tmpl_key, None)
        if template is not None: return template
        # last resort, returnthe generic template for static files
        return self.config.filenames.static
    getStaticFileTemplate = staticFileTemplate

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def staticGridFilename(self, source, region='conus', **kwargs):
        source_path = self.sourceToFilepath(source)
        template = self.getStaticFileTemplate(source)
        template_args = dict(kwargs)
        template_args['region'] = self.regionToFilepath(region, title=False)
        if isinstance(source, ConfigObject):
            config = self.getFiletypeConfig('static.%s' % source.name)
        else: config = self.getFiletypeConfig('static.%s' % source)
        if config is None: filetype = source_path
        else: filetype = config.get('type', source_path)
        template_args['type'] = filetype
        return template % template_args

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def staticGridFilepath(self, source, region='conus', **kwargs):
        static_dir = self.staticWorkingDir()
        filename = self.staticGridFilename(source, region, **kwargs)
        return os.path.join(static_dir, filename)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def staticWorkingDir(self, **kwargs):
        static_dir = self.config.get('dirpaths.static', default=None)
        if static_dir is None:
            working_dir = self.projectRootDir()
            static_dir = os.path.join(working_dir, 'static')
        if not os.path.exists(static_dir):
            if kwargs.get('dir_must_exist',kwargs.get('file_must_exist',False)):
                errmsg = 'Static file directory does not exist :\n%s'
                raise IOError, errmsg % static_dir
            else: os.makedirs(static_dir)
        return static_dir

