
import numpy as N

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class BaseDetector(object):

    def __init__(self, data_type, missing_value):
        self.data_type = data_type
        self.missing_value = missing_value

        if data_type == int and N.isnan(missing_value):
            missing_value = -32768
        if N.isnan(missing_value):
            def _countMissing(sequence):
                return len(N.where(N.isnan(sequence))[0])
            def _isEqual(value_1, value_2):
                if N.isnan(value_1) : return N.isnan(value_2)
                return value_1 == value_2
            def _isMissing(value): return N.isnan(value)
        else:
            def _countMissing(sequence):
                return len(N.where(sequence == missing_value)[0])
            def _isEqual(value_1, value_2): return value_1 == value_2
            def _isMissing(value): return value == missing_value

        self.isEqual = _isEqual
        self.isMissing = _isMissing
        self.countMissing = _countMissing

        self.detected = None
        self.filters = None
        self.filter_groups = None

    # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - -

    def __call__(self, data_array, start_index=0, end_index=None,
                       filters=None):
        """ Wrapper for a required sequence of steps
        """
        self.detected = self.detect(data_array, start_index, end_index)
        self.filters = filters
        if filters is not None: self.filter_groups = self.applyFilters(filters)
        else: self.filter_groups = None
        return self.detected

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def detect(self, data_array, start_index=0, end_index=None):
        if end_index is None: end_index = len(data_array)

        # detect sequences of identical values
        if self.data_type == int:
            data_array[N.where(N.isinf(data_array))] = self.missing_value
            data_array[N.where(N.isnan(data_array))] = self.missing_value
            return self._detect(N.array(data_array, dtype=int), start_index,
                                end_index)
        else:
            return self._detect(data_array, start_index, end_index)

    def _detect(self, data_array, start_index, end_index):
        # do the real work
        raise NotImplementedError

    # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - -

    def applyFilters(self, filters):
        return None

