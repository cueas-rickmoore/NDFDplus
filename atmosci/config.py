
import os, sys

from atmosci.utils.config import ConfigObject

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

ATMOSCFG = ConfigObject('atmosci', None)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# directory paths
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
if 'win32' in sys.platform:
    default = { 'data':'C:\\Work\\app_data',
                'shared':'C:\\Work\\app_data\\shared',
                'static':'C:\\Work\\app_data\\static',
                'working':'C:\\Work' }
else:
    default = { 'data':'/Volumes/data/app_data',
                'shared':'/Volumes/data/app_data/shared',
                'static':'/Volumes/data/app_data/shared/grid/static',
                'working':'/Volumes/data' }

# set the following parameter to the location of temporary forecast files
default['forecast'] = os.path.join(default['shared'], 'forecast')
# set the following parameter to the location of temporary reanalysis files
#default['reanalysis'] = os.path.join(default['shared'], 'reanalysis')

# SET THE CONFIGURED dirpath TO THE default DIRECTORY PATHS
ATMOSCFG.dirpaths = default
# only set the following configuration parameter when multiple apps are
# using the same data source file - set it in each application's config
# file - NEVER set it in the default (global) config file.
# CONFIG.dirpaths.source = CONFIG.dirpaths.shared

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# MODES ALLOW FOR DIFFERENT DIRECTORY PATHS FOR DIFFERENT PURPOSES
ATMOSCFG.modes = { 
    'default':{'dirpaths':default,},
    'dev':{'dirpaths':{
           'data':'/Volumes/Transport/data/app_data',
           'forecast':'/Volumes/Transport/data/app_data/shared/forecast',
           'shared':'/Volumes/Transport/data/app_data/shared',
           'static':'/Volumes/Transport/data/app_data/shared/grid/static',
           'working':'/Volumes/Transport/data'
          },
    },
    'prod':{'dirpaths':default,},
    'test': {'dirpaths':{
             'data':'/Volumes/Transport/data/test_data',
             'forecast':'/Volumes/Transport/data/test_data/shared/forecast',
             'shared':'/Volumes/Transport/data/test_data/shared',
             'static':'/Volumes/Transport/data/test_data/static',
             'working':'/Volumes/Transport/data'
            },
    },
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# regional coordinate bounding boxes for data and maps
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
ATMOSCFG.regions = {
         'conus': { 'description':'Continental United States',
                    'data':'-125.00001,23.99999,-66.04165,49.95834',
                    'maps':'-125.,24.,-66.25,50.' },
         'flny': { 'description':'NY Finger Lakes',
                   'data':'-78.0,42.0,-74.5,47.0',
                   'maps':'-77.9,41.9,-74.6,47.1' },
         'ND': { 'description':'North Dakota (U.S.)',
                 'data':'-105.0,45.0,-95.5,49.98', # ~1 degree data cushion
                 'maps':'-105.2,44.8,-95.3,49.95' }, # ~0.2 degree map cushion
         'NE': { 'description':'NOAA Northeast Region (U.S.)',
                 'data':'-82.75,37.125,-66.83,47.708',
                 'maps':'-82.70,37.20,-66.90,47.60' },
}

