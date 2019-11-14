
from atmosci.acis.griddata import AcisGridDownloadMixin

from atmosci.seasonal.methods.access  import BasicFileAccessorMethods
from atmosci.seasonal.methods.factory import BaseProjectFactory
from atmosci.seasonal.methods.paths   import PathConstructionMethods
from atmosci.seasonal.methods.source  import SourceFileAccessorMethods
from atmosci.seasonal.methods.static  import StaticFileAccessorMethods

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class FactoryGenerator(object):

    def __init__(self):
        self._subclasses = { }
        self._registerSubclasses()

    def __call__(self, class_name, *component_keys):
        components = [ ]
        for key in component_keys:
            components.append(self._components[key])
        return type(class_name, tuple(components), {})

    def registerComponent(self, key, klass):
        self._components[key] = klass

    def _registerComponents(self):
        self.registerComponent('functions', SeasonalProjectFunctions)
        self.registerComponent('base', ProjectFactoryMethods)

