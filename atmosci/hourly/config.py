
import numpy as N

from atmosci.utils.config import ConfigObject

from atmosci.seasonal.config import COMMON

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

CONFIG = COMMON.copy('hourly_config', None)
del COMMON

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# define dataset types
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
CONFIG.datasets.timegrid = { 'chunk_type':('time','gzip'), 
                'dtype':float,
                'dtype_packed':float,
                'frequency':1,
                'missing_data':N.nan,
                'missing_packed':N.nan, 
                'period':'hour',
                'provenance':'timestats',
                'scope':'time',
                'view':('time','lat','lon'),
}
CONFIG.datasets.timegrid.copy('timeaccum', CONFIG.datasets)
CONFIG.datasets.timeaccum.provenance = 'timeaccum'

CONFIG.datasets.timegrid.copy('test', CONFIG.datasets)
CONFIG.datasets.test.description = 'A test dataset.'
CONFIG.datasets.test.tag = 'testdata'
CONFIG.datasets.test.timezone = 'US/Eastern'


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# dataset view maps
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
CONFIG.view_map = {
        ('time','lat','lon'):'tyx', ('lat','lon','time'):'yxt',
        ('time','lon','lat'):'txy', ('lon','lat','time'):'xyt',
        ('time','lat'):'ty', ('time','lon'):'tx',
        ('lat','time'):'yt', ('lon','time'):'xt',
        ('lat','lon'):'yx', ('lon','lat'):'xy',
        ('time',):'t',
}

# set up a couple of filetype and group definitions to use in test files
CONFIG.filetypes = {
        'test':{ 'scope':'time',
                 'datasets':('test','lon','lat'), 
                 'description':'Test Data',
        },
        'group_test':{ 'scope':'time',
                       'groups':('accum','stats'), 
                       'datasets':('lon','lat'), 
                       'description':'Test Groups'
        },
}
CONFIG.groups = {
    'testaccum': { 'description':'Data accum group',
                   'datasets':('test','provenance:timeaccum')
    },
    'teststats': { 'description':'Data stats group',
                   'datasets':('test','provenance:timestats')
    },
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# configure provenance type defintions
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
CONFIG.provenance = {'views':{ 'hour':('hour','time'), 'time':('time','hour')}}
CONFIG.provenance.types = {
    # statistics for time series accumulation 
    'timeaccum': {
        'empty':('',N.nan,N.nan,N.nan,N.nan,N.nan,N.nan,N.nan,N.nan,'',''),
        'formats':[
            '|S14','f4','f4','f4','f4','f4','f4','f4','f4','|S20','|S12'
            ],
        'names':['time','min','max','mean','median', 'min accum','max accum',
                 'mean accum','median accum','processed','source'],
        'period':'hour',
        'scope':'time',
        'type':'timeaccum'
    },

    # simple source time stamp
    'timestamp':{
        'empty':('','',''),
        'formats':['|S10','|S20','|S12'],
        'names':['time','processed','source'],
        'period':'hour',
        'scope':'time',
        'type':'timestamp'
    },

    # simple time series statistics
    'timestats':{
        'empty':('',N.nan,N.nan,N.nan,N.nan,'',''),
        'formats':['|S14','f4','f4','f4','f4','|S20','|S12'],
        'names':['time','min','max','mean','median','processed','source'],
        'period':'hour',
        'scope':'time',
        'type':'timestats'
    },
}

