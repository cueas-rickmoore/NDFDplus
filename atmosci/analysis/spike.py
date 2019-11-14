
import datetime

import numpy as N

from atmosci.analysis.base import BaseDetector

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

DETECT_ERRMSG = "'detect()' method must be called prior to '%s'"

SUPPORTED_ELEMENTS = ('temp','rhum','dewpt','srad','wdir','wspd')

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class SpikeDetector(BaseDetector):

    REPORT_FORMAT = '%s spike @ %%d : magnitude (%%d, %%d) : value = %%d'
    SAVE_FORMAT = '\n%s spike @ %%d : magnitude (%%d, %%d) : value = %%d'

    def __init__(self, data_type, missing_value):
        BaseDetector.__init__(self, data_type, missing_value)
        self.statistics = None

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _detect(self, data_array, start_index, end_index):
        """ Detect all spikes in the data_array
        """
        spikes = [ ]
        start_seq = start_index
        end_seq = start_index + 2

        while end_seq < end_index:
            spike = self._detectSpike(data_array[start_seq:end_seq+1])
            if spike is not None:
                spike_index = start_seq + 1
                spikes.append((spike, spike_index, data_array[spike_index]))
            start_seq += 1
            end_seq += 1

        return tuple(spikes)

    def _detectSpike(self, spike):
        """ A spike is a spike of  3 values where the middle value is
        either higher or lower than the other two values.

        Arguments :
            spike = a spike of 3 values (list, tuple oor Numpy array)

        Returns:
            None = no spike present
            or
            value = aboslute value of the smallest leg of the spike
        """
        # Doesn't work with missing values
        if self.countMissing(spike) > 0: return None
        # no spike if middle value equals the value on either side of it
        if spike[1]==spike[0] or spike[1]==spike[2]: return None

        # difference btween middle value and the value on either side of it
        diff_1_0 = self._difference(spike[:2])
        diff_2_1 = self._difference(spike[1:])

        # no spike if either difference is zero
        if abs(diff_1_0) < 1 or abs(diff_2_1) < 1: return None
        # no spike when both differences have the same sign
        if diff_1_0 > 0 and diff_2_1 > 0: return None
        if diff_1_0 < 0 and diff_2_1 < 0: return None

        # spike occurs when the sign of differences between consecutive
        # values is different ... only need the smaller value of the two diffs
        return diff_1_0, diff_2_1

    def _difference(self, two_values):
        # seems silly, but makes it much easier to build subclasses
        return two_values[1] - two_values[0]

    # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - -

    def applyFilters(self, filters=None):
        if self.detected is None:
            raise RuntimeError, DETECT_ERRMSG % 'applyFilters' 
        # group spikes by filter ... do not use spikes of missing data
        if filters is not None:
            filter_groups = { }
            list_comprehension = "[spike for spike in self.detected %s]"
            for key_, filter_, label_, fmt_ in filters:
                spikes = eval(list_comprehension % filter_)
                if spikes: filter_groups[key_] = tuple(spikes)

        else: filter_groups = None

        # save spike groups as an attribute
        self.filters = filters
        self.filter_groups = filter_groups
        return self.filter_groups

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def calcStatistics(self):
        if self.detected is None:
            raise RuntimeError, DETECT_ERRMSG % 'calcStatistics'
        stats = { }

        if len(self.detected) > 0:
            if self.filter_groups is not None:
                # loop through sequence filters
                for key, flt, lbl, fmt in self.filters:
                    spikes = self.filter_groups.get(key, ())
                    if spikes: stats[key] = self._calcStatistics(spikes)

        # return stats dictionary
        self.statistics = stats
        return stats

    def _calcStatistics(self, spikes):
        magnitudes = [ min(abs(spike[0][0]),abs(spike[0][1]))
                       for spike in spikes ]
        if magnitudes:
            return { 'max' : N.max(magnitudes),
                     'min' : N.min(magnitudes),
                     'median' : N.median(magnitudes),
                     'mean' : N.mean(magnitudes),
                     'stddev' : N.std(magnitudes),
                     'coverage': len(spikes),
                   }
        else:
            # no spike in this data array
            return { 'max':N.nan, 'min':N.nan, 'median':N.nan, 'mean':N.nan,
                     'stddev':N.nan, 'coverage':0 }

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
 
    def reportSpikes(self, spacer='    '):
        if self.detected is None:
            raise RuntimeError, DETECT_ERRMSG % 'reportSpikes'
        return self._reportSpikes(self.detected, spacer)

    def _reportSpikes(self, spikes, spacer='    '):
        fmt = self.REPORT_FORMAT % spacer
        spikes.sort(key=lambda x:x[1])
        for spike in spikes:
            print fmt % (spike[1],spike[0][0],spike[0][1],spike[2])

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def saveSpikes(self, output_file, spacer='    '):
        if self.detected is None:
            raise RuntimeError, DETECT_ERRMSG % 'saveSpikes'
        self._saveSpikes(self, output_file, self.detected, spacer)

    def _saveSpikes(self, output_file, spikes, spacer='    '):
        fmt = self.SAVE_FORMAT % spacer
        for spike in spikes:
            line = fmt % (spike[1],spike[0][0],spile[0][1],spike[2])
            output_file.write(line)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class CircularAngleSpikeDetector(SpikeDetector):

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _difference(self, two_angles):
        # positive angle is always clockwise
        angle = two_angles[1] - two_angles[0]
        if abs(angle) <= 180 : return angle
        if two_angles[0] > two_angles[1]:
            return (two_angles[1] + 360) - two_angles[0] 
        return two_angles[1] - (two_angles[0] + 360)

