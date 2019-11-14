

import numpy as N

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

NEIGHBORHOOD = ( (-1,0),(1,0),(0,1),(0,-1),(1,1),(1,-1),
                 (-1,1),(-1,-1),(0,2),(0,-2),(2,0),(-2,0) )

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class ArrayAnomalyFinder(object):

    def __init__(self, data_key, data_manager, search_radius, report_file=None):
        self.data_key = data_key
        self.data_manager = data_manager
        
        data, attrs = data_manager.getData(data_key)
        self._data = data
        self._lons, self._lats = data_manager.getLonLat()
        self._data_units = attrs.get('units',None)
        
        self.search_radius = search_radius
        self.report_file = report_file

    def belowMinThreshold(self, threshold, allowed_deviation=1.):
        suspicious = N.where(self._data <= threshold)
        if len(self._data.shape) > 1:
            return self._analyze(list(zip(*suspicious)), allowed_deviation)
        else:
            return self._analyze(suspicious, allowed_deviation)

    def aboveMaxThreshold(self, threshold, allowed_deviation=1.):
        suspicious = N.where(self._data >= threshold)
        if len(self._data.shape) > 1:
            return self._analyze(list(zip(*suspicious)), allowed_deviation)
        else:
            return self._analyze(suspicious, allowed_deviation)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def _analyze(self, suspicious, allowed_deviation):
        indexes = []
        values = []
        anomaly = 0 

        for indx in suspicious:
            if self.report_file is not None:
                anomaly += 1
                self.report_file.write('\n\nsuspicious value %d :' % anomaly)
                self._reportPointData(indx, '???')

            suspicious_value = self._getDataValue(indx)
            neighbor_data = self._getNeighborData(indx)
            # must have more than 2 neighbors for the statistics to make sense
            if len(neighbor_data) > 2:
                neighbor_data = N.array(neighbor_data)
                neighbor_mean = N.mean(neighbor_data)
                neighbor_stddev = N.std(neighbor_data)
                deviation = suspicious_value - neighbor_mean
                tolerance = (allowed_deviation * neighbor_stddev)
                if self.report_file is not None:
                    value = '%8.3f' % neighbor_mean
                    self.report_file.write('\n>>> neighbor mean = %s' %
                                           value.strip())
                    value = '%8.3f' % neighbor_stddev
                    self.report_file.write('\n>>> standard deviation = +-%s' %
                                           value.strip())
                    value = '%8.3f' % tolerance
                    self.report_file.write('\n>>> allowed deviation = +-%s' %
                                           value.strip())
                    value = '%8.3f' % deviation
                    self.report_file.write('\n>>> actual deviation = %s' %
                                           value.strip())
                if abs(deviation) > tolerance:
                    indexes.append(indx)
                    values.append(suspicious_value)

        return values, indexes

    def _getDataValue(self, indx):
        return self._data[indx]

    def _getNeighborData(self, indx):
        radius = self.search_radius
        lon = self._lons[indx]
        lat = self._lats[indx]
        indexes = N.where( (self._lons >= lon - radius) &
                           (self._lons <= lon + radius) &
                           (self._lats >= lat - radius) &
                           (self._lats <= lat + radius) )
        indexes = [_indx for _indx in indexes[0] if _indx != indx]

        if self.report_file is not None:
            for _indx in indexes:
                self._reportPointData(_indx)

        return self._data[indexes]

    def _reportPointData(self, indx, leadin='...'):
        self.report_file.write('\n%s %d  %7.3f  %8.3f  %6.3f' %
                               (leadin, indx, self._data[indx],
                               self._lons[indx], self._lats[indx]) )


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class GridAnomalyFinder(ArrayAnomalyFinder):

    def __init__(self, data_key, data_manager, report_file=None,
                       neighborhood=NEIGHBORHOOD):
        ArrayAnomalyFinder.__init__(self, data_key, data_manager, 0.,
                                    report_file)
        self._neighborhood = neighborhood
        self._size_of_neighborhood = len(neighborhood)

    def _getDataValue(self, indx):
        return self._data[indx[0],indx[1]]

    def _getNeighborData(self, indx):
        index_0 = indx[0]
        index_1 = indx[1]
        neighbor_data = [ ]

        for _indx in range(self._size_of_neighborhood):
            offset_0, offset_1 = self._neighborhood[_indx]
            n_index_0 = index_0 + offset_0
            n_index_1 = index_1 + offset_1
            try:
                value = self._data[n_index_0,n_index_1]
            except:
                continue
            if self.report_file is not None:
                self._reportPointData((n_index_0,n_index_1))

            # only add if the data is real
            if N.isfinite(value):
                neighbor_data.append(value)

        return neighbor_data

    def _reportPointData(self, indx, leadin='...'):
        index_0 = indx[0]
        index_1 = indx[1]
        self.report_file.write('\n%s %d  %d  %7.3f  %8.3f  %6.3f' %
                               (leadin, index_0, index_1,
                                self._data[index_0,index_1],
                                self._lons[index_0,index_1],
                                self._lats[index_0,index_1]) )
