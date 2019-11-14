
import numpy as N

from atmosci.analysis.base import BaseDetector

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

DETECT_ERRMSG = "either 'detect()' or call method must be run prior to '%s'"
FILTER_ERRMSG = "No sequence grouping filters specified."
GROUP_ERRMSG = "either 'groupSequences()' or call method must be run prior to '%s'"

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class SequenceDetector(BaseDetector):

    REPORT_FORMAT = '%s%%d occurences starting @ %%d = %%s'
    SAVE_FORMAT = '\n%s%%d occurences starting @ %%d = %%s'

    def __init__(self, data_type, missing_value, tolerance=N.inf):

        BaseDetector.__init__(self, data_type, missing_value)
        self.detected = None
        self.filter_groups = None
        self.statistics = None

        self.tolerance = tolerance
        if N.isinf(tolerance):
            def _isEquivalent(value_1, value_2):
                return value_1 == value_2
        elif tolerance == 0:
            def _isEquivalent(value_1, value_2):
                return (int(value_1) - int(value_2)) == 0
        elif isinstance(tolerance, int):
            _tolerance = eval('1e-%d' % tolerance)
            def _isEquivalent(value_1, value_2):
                return abs(value_1 - value_2) <= _tolerance
            self.tolerance = _tolerance
        else:
            def _isEquivalent(value_1, value_2):
                return abs(value_1 - value_2) <= tolerance
        self._isEquivalent = _isEquivalent

    # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - -

    def _detect(self, data_array, start_index, end_index):
        runs = [ ]
        count = 1
        indx = start_index
        previous = data_array[0]

        while indx < end_index:
            value = data_array[indx]
            if self.isMissing(previous) and self.isMissing(value):
                count += 1
            elif self._isEquivalent(value, previous):
                count += 1
            else:
                if count > 1:
                    runs.append( (previous, count, indx-1) )
                previous = value
                count = 1
            indx += 1

        return tuple(runs)

    # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - -

    def applyFilters(self, filters=None):
        if self.detected is None:
            raise RuntimeError, DETECT_ERRMSG % 'applyFilters'
        if filters is None: self.filters = [ ]
        filter_groups = { }

        # group sequences by filter ... do not use sequences of missing data
        valid = [ run for run in self.detected if not self.isMissing(run[0]) ]
        if valid:
            if filters is not None:
                for key_, filter_, label_, fmt_ in filters:
                    if key_ == 'missing': continue
                    if filter_:
                        runs = eval("[ run for run in valid %s]" % filter_)
                        if runs: filter_groups[key_] = tuple(runs)
                    else:
                        filter_groups[key_] = valid
            else:
                filter_groups['x==x'] = valid
                self.filters.append(('x==x','','valid values',lambda x:str(x)))

        # sequences of missing data
        missing = [ run for run in self.detected if self.isMissing(run[0]) ]
        if missing:
            filter_groups['missing'] = tuple(missing)
            if filters is None:
                self.filters.append( ('missing','','missing values',
                                                'missing') )
        # save sequence groups as an attribute
        self.filter_groups = filter_groups
        return self.filter_groups

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def calcStatistics(self):
        if self.detected is None:
            raise RuntimeError, DETECT_ERRMSG % 'calcStatistics'
        stats =  { }

        if len(self.detected) > 0:
            if self.filter_groups is not None:
                # loop through sequence filters
                for key, flt, lbl, fmt in self.filters:
                    runs = self.filter_groups.get(key, ())
                    if runs: stats[key] = self._calcStatistics(runs)

        # return stats dictionary
        self.statistics = stats
        return stats

    def _calcStatistics(self, sequences):
        counts = [ run[1] for run in sequences ]
        if counts: return { 'max' : N.max(counts), 'min' : N.min(counts),
                            'median' : N.median(counts),
                            'mean' : int(round(N.mean(counts))),
                            'stddev' : max(int(round(N.std(counts))),1),
                            'coverage': len(counts),
                           }
        # no runs in this sequence set
        else: return { 'max':0, 'min':0, 'median':0, 'mean':0, 'stddev':0,
                       'coverage' : 0 }

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def reportSequences(self, include_missing=True, count_cutoff=''):
        if self.detected is None:
            raise RuntimeError, DETECT_ERRMSG % 'reportSequences'
        if count_cutoff and self.statistics is None:
            raise RuntimeError, DETECT_ERRMSG % 'reportSequences'
        summary = '\n    sequences of %s :'

        if self.filter_groups is None: self.applyFilters()
        filter_groups = self.filter_groups

        # loop through sequence filters
        for params in self.filters:
            if params[0] == 'missing':
                missing_filter = params
                continue
            key, f, label, fmt = params
            runs = filter_groups.get(key, ())
            if runs and count_cutoff:
                statistic = self.statistics[count_cutoff]
                runs = [run for run in runs if run[1] > statistic]
            if runs:
                print summary % label
                self._reportSequences(self._formatValues(runs, fmt),
                                      '        ')
        # runs of missing values
        if include_missing:
            runs = filter_groups.get('missing', ())
            if runs:
                k, f, label, fmt = missing_filter
                print summary % label
                self._reportSequences(self._formatValues(runs, fmt), '        ')

    def _reportSequences(self, sequences, spacer='    '):
        fmt = self.REPORT_FORMAT % spacer
        sequences.sort(key=lambda x:x[2])
        for run in sequences:
            print fmt % (run[1],run[2],run[0])

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def saveSequences(self, output_file, include_missing=True, count_cutoff=''):
        if self.detected is None:
            raise RuntimeError, DETECT_ERRMSG % 'saveSequences'
        summary = '\n    sequences of %s :'  

        if self.filter_groups is None: self.applyFilters()
        filter_groups = self.filter_groups

        # loop through sequence filters
        for params in self.filters:
            if params[0] == 'missing':
                missing_filter = params
                continue
            key, f, label, fmt = params
            runs = filter_groups.get(key, ())
            if runs and count_cutoff:
                statistic = self.statistics['%s_count' % count_cutoff]
                runs = [run for run in runs if run[1] > statistic]
            if runs: 
                output_file.write(summary % label_)
                self._saveSequences(output_file, self._formatValues(runs, fmt),
                                    '        ')

        # runs of missing values
        if include_missing:
            runs = filter_groups.get('missing', ())
            if runs:
                k, f, label, fmt = missing_filter
                output_file.write(summary % label)
                self._saveSequences(output_file, self._formatValues(runs, fmt),
                                    '        ')

    def _saveSequences(self, output_file, sequences, spacer='    '):
        fmt = self.SAVE_FORMAT % spacer
        sequences.sort(key=lambda x:x[2])
        for run in sequences:
            output_file.write(fmt % (run[1],run[2],run[0]))

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _formatValues(self, sequences, format_=None):
        if format_ is None: format_ = str

        if callable(format_):
            return [(format_(run[0]), run[1], run[2]) for run in sequences]
        elif isinstance(format_, basestring):
            if '%' in format_:
                return [(format_ % run[0], run[1], run[2]) for run in sequences]
            else:
                return [(format_, run[1], run[2]) for run in sequences]

        raise ValueError, "Invalid value formatter : %s" % str(format_)

