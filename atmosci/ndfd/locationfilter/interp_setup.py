# build the interp.so library in place using the command:
#     ==> python interp_setup.py build_ext --inplace

from distutils.core import setup
from distutils.extension import Extension
import numpy

ext_modules = [Extension("interp", sources=["interp.c"],
                         include_dirs=[numpy.get_include()],),
              ]

setup(
  name = 'nrcc.math',
  description = 'Custom Interpolation Routines',
  ext_modules = ext_modules
)

