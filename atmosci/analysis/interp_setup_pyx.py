# build the interp.so library in place using the command:
#     ==> python interp_setup.py build_ext --inplace

from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext
import numpy


ext_modules = [Extension("interp", ["interp.pyx"])]

setup(
  name = 'Custom Interpolation Routines',
  cmdclass = {'build_ext': build_ext},
  include_dirs = [numpy.get_include()],
  ext_modules = ext_modules
)

