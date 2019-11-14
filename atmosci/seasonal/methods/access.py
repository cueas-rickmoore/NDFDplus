

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class BasicFileAccessorMethods:
    """ Basic methods for finding and instantiating the correct file
    accessor for a specific filetype / source / region / timespan
    combination from amongst all accessors registered to the factory.

    REQUIRED:
        1. must be included in a class derived from 
           atmosci.seasonal.factory.BaseProjectFactory
        2. subclasses must register 'read', 'write' and 'build' accessor
           classes for each project filetype using either:
              self._registerAccessManagers(filetype, ReaderClass,
                                           ManagerClass, BuilderClass)
              OR
              self._registerAccessManager(filetype, 'read', ReaderClass)
              self._registerAccessManager(filetype, 'manage', ManagerClass)
              self._registerAccessManager(filetype, 'build', BuilderClass)
    """

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def fileAccessorClass(self, filetype_key, access_type):
        filetype_obj = self.getFiletypeConfig(filetype_key)
        if filetype_obj is not None:
            class_type = filetype_obj.get('filetype', filetype_key)
        else:
            if '.' in filetype_key:
                class_type = filetype_key.split('.')[0]
            else: class_type = filetype_key
        Classes = self.AccessClasses.get(class_type, None)
        if Classes is None: Classes = self.AccessClasses.default
        return Classes[access_type]
    getFileAccessorClass = fileAccessorClass

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # Generic grid file access manager constructors
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def gridFileBuilder(self, filepath, filetype, source, target_year,
                                 region=None, **kwargs):
        return self.newProjectFileBuilder(filepath, filetype, source,
                                          target_year, region, **kwargs)
    getGridFileBuilder = gridFileBuilder

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def gridFileManager(self, filepath, filetype='project', mode='r'):
        return self.newProjectFileAccessor(filepath, 'manage', filetype, mode)
    getGridFileManager = gridFileManager

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def gridFileReader(self, filepath, filetype='project'):
        return self.newProjectFileAccessor(filepath, 'read', filetype)
    getGridFileReader = gridFileReader

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def newProjectFileAccessor(self, filepath, access, filetype, mode=None):
        Class = self.fileAccessorClass(filetype, access)
        registry = self.getRegistryConfig()
        if access == 'read': return Class(filepath, registry)
        else: return Class(filepath, registry, mode)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def newProjectFileBuilder(self, filepath, filetype, source, target_year,
                                    region, **kwargs):
        registry = kwargs.get('registry', self.getRegistryConfig())
        config = kwargs.get('config', self.config)
        Class = self.fileAccessorClass(filetype, 'build')
        config = self.config
        if target_year is not None:
            if kwargs:
                return Class(filepath, registry, config, filetype, source,
                             target_year, region, **kwargs)
            else:
                return Class(filepath, registry, config, filetype, source,
                             target_year, region)
        else:
            if kwargs:
                return Class(filepath, registry, config, filetype, source,
                             region, **kwargs)
            else:
                return Class(filepath, registry, config, filetype, source,
                             region)


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # should be implemented by any Factory subclass to ensoure that any
    # custom file accessors are made known to the Factory.
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _registerAccessClasses(self):
        """ Convenience function to register access classes for all content
        types. Called by the constructor via the private function
        _initFileManagerClasses_()

        """
        pass

