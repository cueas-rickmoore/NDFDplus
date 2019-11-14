
import math

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class AcisGridNodeFinder(object):

    def __init__(self, region='conus'):
	self.spacing = 0.041667

        if region.lower() == 'ne':
            self.min_lat = 37.125
            self.min_lon =  -82.75
        else: # default to continental US
            self.min_lat = 24.0
            self.min_lon = -125.0
        self.abs_lon = abs(self.min_lon)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def __call__(self, lon, lat, precision):
        return ( self.nearestNodeLon(lon, precision),
                 self.nearestNodeLat(lat, precision) )

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def nearestNodeLon(self, lon, precision=3):
        spacing = self.spacing
	diff = self.abs_lon + lon
	offset = round(diff / spacing) * spacing
	node_lon =  self.min_lon + offset
	return round(node_lon, precision)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def nearestNodeLat(self, lat, precision=3):
        spacing = self.spacing
	diff = lat - self.min_lat
	offset = round(diff / spacing) * spacing
	node_lat = self.min_lat + offset
	return round(node_lat, precision)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class AcisGridNodeIndexer(AcisGridNodeFinder):

    def __call__(self, lon, lat):
        return self.nearestNode(lon, lat)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def nearestNode(self, lon, lat, precision=3, indexes_only=True):
        spacing = self.spacing
        near_lon = self.nearestNodeLon(lon, precision)
        x = int(round((self.abs_lon + near_lon) / spacing))
        near_lat = self.nearestNodeLat(lat, precision)
        y = int(round((near_lat - self.min_lat) / spacing))
        if indexes_only: return y, x
        else: return y, x, near_lon, near_lat

