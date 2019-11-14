import os

from setuptools import setup, find_packages
from distutils.extension import Extension
import numpy

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

BUILD_DIR = os.path.dirname(__file__)

def getVersion():
    return open(os.path.join(BUILD_DIR,'nrcc','version.txt')).read().strip()

def read(*filepath):
    return open(os.path.join(BUILD_DIR, *filepath)).read()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

PKG_PATH = os.path.normpath(os.getcwd())
PACKAGES = find_packages(exclude=('*.data', '*.data.*',))
DATA_PACKAGES = [os.path.join('cac','data','web_services.wsdl'),
                 os.path.join('compag','cron','compag_backlog.py'),
                 os.path.join('compag','cron','compag_daemon.py'),
                 os.path.join('compag','cron','confirm_temp_download.py'),
                 os.path.join('compag','cron','download_temp_data.py'),
                 os.path.join('compag','cron','build_temp_extremes_grids.py'),
                 os.path.join('compag','cron','build_precip_grids.py'),
                 os.path.join('compag','cron','download_precip_data.py'),
                 os.path.join('compag','cron','new_acis_grid_file.py'),
                 os.path.join('ruc','data','qc_tables.py'),
                 os.path.join('stations','cron','create_station_cache.py'),
                ]

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # "

dist = setup(name='nrcc', version=getVersion(),
             description='NRCC grid build & utilities',
             author='Rick Moore',
             setup_requires=['numpy',],
             ext_modules = [ Extension("nrcc.analysis.interp",
                             sources=["nrcc/analysis/interp.c"],
                             include_dirs=[numpy.get_include()],
                             libraries=["m"],),
                           ],
             packages=PACKAGES,
             package_data={ 'nrcc' : DATA_PACKAGES },
           )

