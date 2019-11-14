
import os, sys

import numpy as N
from scipy import stats as scipy_stats
try:
    nanmedian = N.nanmedian
except:
    nanmedian = scipy_stats.nanmedian

from atmosci.utils.config import ConfigObject
from atmosci.utils.timeutils import asAcisQueryDate

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

REGBASE = ConfigObject('registry', None)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# dataset function registry
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
FUNCBASE = ConfigObject('functions', REGBASE)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# dataset creators
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
ConfigObject('creators', FUNCBASE)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# provenance generators
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
ConfigObject('generators', FUNCBASE)

# record generator for date series - data with accumulation
def dateAccumStatsProvenanceGenerator(date, timestamp, daily, accumulated):
    return ( asAcisQueryDate(date), N.nanmin(daily), N.nanmax(daily),
             N.nanmean(daily), nanmedian(daily,axis=None),
             N.nanmin(accumulated), N.nanmax(accumulated),
             N.nanmean(accumulated), nanmedian(accumulated,axis=None),
             timestamp )
FUNCBASE.generators.dateaccum = dateAccumStatsProvenanceGenerator

# record generator for day of year series - data with accumulation
def doyAccumStatsProvenanceGenerator(doy, timestamp, daily, accumulated):
    return ( doy, N.nanmin(daily), N.nanmax(daily), N.nanmean(daily),
             nanmedian(daily,axis=None), N.nanmin(accumulated),
             N.nanmax(accumulated), N.nanmean(accumulated),
             nanmedian(accumulated,axis=None), timestamp ) 
FUNCBASE.generators.doyaccum = doyAccumStatsProvenanceGenerator

# record generator for date series statistics - no accumulation
def dateStatsProvenanceGenerator(date, timestamp, data):
    return ( asAcisQueryDate(date), N.nanmin(data), N.nanmax(data),
             N.nanmean(data), nanmedian(data,axis=None), timestamp )
FUNCBASE.generators.datestats = dateStatsProvenanceGenerator

# record generator for day of year series statistics - no accumulation
def doyStatsProvenanceGenerator(doy, timestamp, data):
    return ( doy, N.nanmin(data), N.nanmax(data), N.nanmean(data), 
             nanmedian(data,axis=None), timestamp )
FUNCBASE.generators.doystats = doyStatsProvenanceGenerator

# record generator for observed date series
def observedProvenanceGenerator(date, timestamp, data):
    return ( asAcisQueryDate(date), N.nanmin(data), N.nanmax(data), 
             N.nanmean(data), nanmedian(data,axis=None), timestamp )
FUNCBASE.generators.observed = observedProvenanceGenerator

# record generator for min/max temperature estremes group
def tempExtremesProvenanceGenerator(date, timestamp, mint, maxt, source):
    return ( asAcisQueryDate(date),
             N.nanmin(mint), N.nanmax(mint), N.nanmean(mint),
             N.nanmin(maxt), N.nanmax(maxt), N.nanmean(maxt),
             source, timestamp )
FUNCBASE.generators.tempexts = tempExtremesProvenanceGenerator

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# dataset indexers
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
ConfigObject('indexers', FUNCBASE)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# dataset readers
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
ConfigObject('readers', FUNCBASE)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# dataset writers
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
ConfigObject('writers', FUNCBASE)

