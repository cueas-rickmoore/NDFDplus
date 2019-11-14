
from atmosci.acis.griddata import AcisGridDownloadMixin

from atmosci.seasonal.methods.access  import BasicFileAccessorMethods
from atmosci.seasonal.methods.factory import BaseProjectFactory
from atmosci.seasonal.methods.ndfd    import NDFDFactoryMethods
from atmosci.seasonal.methods.paths   import PathConstructionMethods
from atmosci.seasonal.methods.source  import SourceFileAccessorMethods
from atmosci.seasonal.methods.static  import StaticFileAccessorMethods

from atmosci.seasonal.grid import SeasonalGridFileBuilder
from atmosci.seasonal.grid import SeasonalGridFileManager
from atmosci.seasonal.grid import SeasonalGridFileReader


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from atmosci.seasonal.config import CFGBASE
from atmosci.seasonal.registry import REGBASE


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def registerSourceAccessManagers(self):
    self._registerAccessManagers('source', SeasonalGridFileReader,
                                           SeasonalGridFileManager,
                                           SeasonalGridFileBuilder)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def registerStaticAccessManagers(self):
    from atmosci.seasonal.static import StaticGridFileBuilder
    from atmosci.seasonal.static import StaticGridFileManager
    from atmosci.seasonal.static import StaticGridFileReader

    self._registerAccessManagers('static', StaticGridFileReader,
                                           StaticGridFileManager,
                                           StaticGridFileBuilder)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class BasicSeasonalProjectFactory(PathConstructionMethods,
                                  BasicFileAccessorMethods,
                                  BaseProjectFactory):

    def __init__(self, config=CFGBASE, registry=REGBASE):
        BaseProjectFactory.__init__(self, config, registry)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # register default file accessors
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _registerAccessClasses(self):
        self._registerAccessManagers('default', SeasonalGridFileReader,
                                     SeasonalGridFileManager,
                                     SeasonalGridFileBuilder)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class SeasonalSourceFileFactory(SourceFileAccessorMethods,
                                BasicSeasonalProjectFactory):
    """ Provides methods for generating source data file paths and
    instatiating source data file accessors and builders. It also
    provides comprehensive access to all definitions contained in
    a project's configuration file.

    The registry is not used in this version, but is included for use
    in a future feature set.
    """

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # register accessors for source files
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _registerAccessClasses(self):
        BasicSeasonalProjectFactory._registerAccessClasses(self)
        registerSourceAccessManagers(self)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class SeasonalStaticFileFactory(StaticFileAccessorMethods,
                                BasicSeasonalProjectFactory):
    """ Provides methods for generating static data file paths and
    instantiating static data file accessors and builders. It also
    provides comprehensive access to all definitions contained in
    the project's configuration file.

    The registry is not used in this version, but is included for use
    in a future feature set.
    """

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # register accessors for static files
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _registerAccessClasses(self):
        BasicSeasonalProjectFactory._registerAccessClasses(self)
        registerStaticAccessManagers(self)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class SeasonalProjectFactory(SourceFileAccessorMethods,
                             StaticFileAccessorMethods,
                             BasicSeasonalProjectFactory):
    """ Provides methods for generating both source and static data file
    paths and instantiating data file accessors and builders for both
    file types. It also provides comprehensive access to all definitions
    contained in the project's configuration file.

    The registry is not used in this version, but is included for use
    in a future feature set.
    """

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # register accessors for source and static files
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _registerAccessClasses(self):
        BasicSeasonalProjectFactory._registerAccessClasses(self)
        registerSourceAccessManagers(self)
        registerStaticAccessManagers(self)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class AcisProjectFactory(AcisGridDownloadMixin, SeasonalProjectFactory):
    """ Adds ACIS grid downloading capabilities to the SeasonalProjectFactory
    class.
    """

    def __init__(self, config=CFGBASE, registry=REGBASE):
        SeasonalProjectFactory.__init__(self, config, registry)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class NDFDProjectFactory(NDFDFactoryMethods, SeasonalProjectFactory):

    def __init__(self, config=CFGBASE, registry=REGBASE, alt_server_url=None):
        SeasonalProjectFactory.__init__(self, config, registry)
        if alt_server_url is None: self.initNDFD()
        else: self.initNDFD(alt_server_url)

