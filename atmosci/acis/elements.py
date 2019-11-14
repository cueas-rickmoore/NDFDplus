""" data element properties
"""

import numpy as N

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from atmosci.base.manager import OBSERVED_ELEV, OBSERVED_PREFIX
from atmosci.base.manager import MAXTEMP_DATASET, OBSERVED_MAXTEMP
from atmosci.base.manager import MINTEMP_DATASET, OBSERVED_MINTEMP
from atmosci.base.manager import PRECIP_DATASET, OBSERVED_PRECIP

MAXTEMP_OBS_FLAG = MAXTEMP_DATASET + '_obs_flag'
MINTEMP_OBS_FLAG = MINTEMP_DATASET + '_obs_flag'
PRECIP_OBS_FLAG  = PRECIP_DATASET + '_obs_flag'

OBS_FLAG_KEYS = { MAXTEMP_DATASET : MAXTEMP_OBS_FLAG,
                  MINTEMP_DATASET : MINTEMP_OBS_FLAG,
                  PRECIP_DATASET  : PRECIP_OBS_FLAG }

MAXTEMP_OBS_TIME = MAXTEMP_DATASET + '_obs_hour'
MINTEMP_OBS_TIME = MINTEMP_DATASET + '_obs_hour'
PRECIP_OBS_TIME  = PRECIP_DATASET + '_obs_hour'

OBS_TIME_KEYS = { MAXTEMP_DATASET : MAXTEMP_OBS_TIME,
                  MINTEMP_DATASET : MINTEMP_OBS_TIME,
                  PRECIP_DATASET  : PRECIP_OBS_TIME }

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

DATA_TYPES = { }
DATA_TYPES['avgt'] = N.dtype(float)
DATA_TYPES['cdd'] = N.dtype(float)
DATA_TYPES['county'] = N.dtype('a5')
DATA_TYPES['elev'] = N.dtype(float)
DATA_TYPES['gdd'] = N.dtype(float)
DATA_TYPES['gdd40'] = N.dtype(float)
DATA_TYPES['gdd50'] = N.dtype(float)
DATA_TYPES['hdd'] = N.dtype(float)
DATA_TYPES['lat'] = N.dtype(float)
DATA_TYPES['lon'] = N.dtype(float)
DATA_TYPES['name'] = N.dtype('object')
DATA_TYPES['ncdc'] = N.dtype('a2')
DATA_TYPES['obst'] = N.dtype(float)
DATA_TYPES['postal'] = N.dtype('a2')
DATA_TYPES['snow'] = N.dtype(float)
DATA_TYPES['snwd'] = N.dtype(float)
DATA_TYPES['uid'] = N.dtype('int64')
DATA_TYPES[MAXTEMP_DATASET] = N.dtype(float)
DATA_TYPES[MAXTEMP_OBS_FLAG] = N.dtype('a1')
DATA_TYPES[MAXTEMP_OBS_TIME] = N.dtype('<i2')
DATA_TYPES[MINTEMP_DATASET] = N.dtype(float)
DATA_TYPES[MINTEMP_OBS_FLAG] = N.dtype('a1')
DATA_TYPES[MINTEMP_OBS_TIME] = N.dtype('<i2')
DATA_TYPES[OBSERVED_ELEV] = N.dtype(float)
DATA_TYPES[OBSERVED_MAXTEMP] = N.dtype(float)
DATA_TYPES[OBSERVED_MINTEMP] = N.dtype(float)
DATA_TYPES[OBSERVED_PRECIP] = N.dtype(float)
DATA_TYPES[PRECIP_DATASET] = N.dtype(float)
DATA_TYPES[PRECIP_OBS_FLAG] = N.dtype('a1')
DATA_TYPES[PRECIP_OBS_TIME] = N.dtype('<i2')

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

DATA_UNITS = { }
DATA_UNITS['avgt'] = 'F'
DATA_UNITS['cdd'] = 'days'
DATA_UNITS['elev'] = 'ft'
DATA_UNITS['gdd'] = 'days'
DATA_UNITS['gdd40'] = 'days'
DATA_UNITS['gdd50'] = 'days'
DATA_UNITS['hdd'] = 'days'
DATA_UNITS['lat'] = 'DD'
DATA_UNITS['lon'] = 'DD'
DATA_UNITS['obst'] = 'F'
DATA_UNITS['snow'] = 'in'
DATA_UNITS['snwd'] = 'in'
DATA_UNITS[MAXTEMP_DATASET] = 'F'
DATA_UNITS[MAXTEMP_OBS_TIME] = 'hours since midnight local time'
DATA_UNITS[MINTEMP_DATASET] = 'F'
DATA_UNITS[MINTEMP_OBS_TIME] = 'hours since midnight local time'
DATA_UNITS[OBSERVED_ELEV] = 'ft'
DATA_UNITS[OBSERVED_MAXTEMP] = 'F'
DATA_UNITS[OBSERVED_MINTEMP] = 'F'
DATA_UNITS[OBSERVED_PRECIP] = 'in'
DATA_UNITS[PRECIP_DATASET] = 'in'
DATA_UNITS[PRECIP_OBS_TIME] = 'hours since midnight local time'

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

DESCRIPTIONS = { }
DESCRIPTIONS['avgt'] = 'average temperature'
DESCRIPTIONS['cdd'] = 'heating degree days (base 65)'
DESCRIPTIONS['county'] = '5 digit FIPS county code' 
DESCRIPTIONS['elev'] = 'elevation'
DESCRIPTIONS['gdd'] = 'growing degree days (base 50)'
DESCRIPTIONS['gdd40'] = 'growing degree days (base 40)'
DESCRIPTIONS['gdd50'] = 'growing degree days (base 50)'
DESCRIPTIONS['hdd'] = 'heating degree days (base 65)'
DESCRIPTIONS['name'] = 'station location name'
DESCRIPTIONS['ncdc'] = '2 digit NCDC code for state'
DESCRIPTIONS['lat'] = 'latitude'
DESCRIPTIONS['lon'] = 'longitude'
DESCRIPTIONS['obst'] = 'temperature at observation time'
DESCRIPTIONS['postal'] = '2 character USPS code for state'
DESCRIPTIONS['snow'] = 'snowfall'
DESCRIPTIONS['snwd'] = 'snow depth'
DESCRIPTIONS['uid'] = 'ACIS unique identifier for station'
DESCRIPTIONS[MAXTEMP_DATASET] = 'maximum temperature'
DESCRIPTIONS[MAXTEMP_OBS_TIME] = 'hour when max temp was reported'
DESCRIPTIONS[MINTEMP_DATASET] = 'minimum temperature'
DESCRIPTIONS[MINTEMP_OBS_TIME] = 'hour when min temp was reported'
DESCRIPTIONS[OBSERVED_ELEV] = 'elevation as reported by ACIS Web Services.'
DESCRIPTIONS[OBSERVED_MAXTEMP] = 'maximum temperate as reported by ACIS Web Services.'
DESCRIPTIONS[OBSERVED_MINTEMP] = 'minimum temperate as reported by ACIS Web Services.'
DESCRIPTIONS[OBSERVED_PRECIP] = 'precipitation as reported by ACIS Web Services.'
DESCRIPTIONS[PRECIP_DATASET] = 'precipitation'
DESCRIPTIONS[PRECIP_OBS_TIME] = 'hour when precipitation was reported'

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

MASKED = { }
MASKED['lat'] = N.nan
MASKED['lon'] = N.nan
MASKED['elev'] = N.nan
MASKED['avgt'] = N.nan
MASKED['obst'] = N.nan
MASKED['snow'] = N.nan
MASKED['snwd'] = N.nan
MASKED['cdd'] = N.nan
MASKED['gdd'] = N.nan
MASKED['gdd40'] = N.nan
MASKED['gdd50'] = N.nan
MASKED['hdd'] = N.nan
MASKED[MAXTEMP_DATASET] = N.nan
MASKED[MINTEMP_DATASET] = N.nan
MASKED[OBSERVED_ELEV] = N.nan
MASKED[OBSERVED_MAXTEMP] = N.nan
MASKED[OBSERVED_MINTEMP] = N.nan
MASKED[OBSERVED_PRECIP] = N.nan
MASKED[PRECIP_DATASET] = N.nan

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

MISSING = { }
MISSING['lat'] = N.inf
MISSING['lon'] = N.inf
MISSING['elev'] = N.inf
MISSING['avgt'] = N.inf
MISSING['obst'] = N.inf
MISSING['snow'] = N.inf
MISSING['snwd'] = N.inf
MISSING['cdd'] = N.inf
MISSING['gdd'] = N.inf
MISSING['gdd40'] = N.inf
MISSING['gdd50'] = N.inf
MISSING['hdd'] = N.inf
MISSING[MAXTEMP_DATASET] = N.inf
MISSING[MAXTEMP_OBS_FLAG] = ' '
MISSING[MAXTEMP_OBS_TIME] = '-1'
MISSING[MINTEMP_DATASET] = N.inf
MISSING[MINTEMP_OBS_FLAG] = ' '
MISSING[MINTEMP_OBS_TIME] = '-1'
MISSING[OBSERVED_ELEV] = N.inf
MISSING[OBSERVED_MAXTEMP] = N.inf
MISSING[OBSERVED_MINTEMP] = N.inf
MISSING[OBSERVED_PRECIP] = N.inf
MISSING[PRECIP_DATASET] = N.inf
MISSING[PRECIP_OBS_FLAG] = ' '
MISSING[PRECIP_OBS_TIME] = '-1'

