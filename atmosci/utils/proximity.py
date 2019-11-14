
import math
import sys

import numpy as N

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

RELATIVE_INDEXES = {  9 : ( (-1, -1, -1,  0, 0, 0,  1, 1, 1),
                            (-1,  0,  1, -1, 0, 1, -1, 0, 1) )
                   , 13 : ( (-2, -1, -1, -1,  0,  0, 0, 0, 0,  1, 1, 1, 2),
                            ( 0, -1,  0,  1, -2, -1, 0, 1, 2, -1, 0, 1, 0) )
                   , 25 : ( (-3, -2, -2, -2, -1, -1, -1, -1, -1,
                              0,  0,  0,  0,  0,  0,  0,
                              1,  1,  1,  1,  1,  2,  2,  2,  3),
                            ( 0, -1,  0,  1, -2, -1,  0,  1,  2,
                             -3, -2, -1,  0,  1,  2,  3,
                             -2, -1,  0,  1,  2, -1,  0,  1,  0) )
                   }

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def indexOfClosestNode(target_lon, target_lat, lon_grid, lat_grid, radius):
    indexes = N.where( (lon_grid >= (target_lon-radius)) &
                       (lon_grid <= (target_lon+radius)) &
                       (lat_grid >= (target_lat-radius)) &
                       (lat_grid <= (target_lat+radius)) )
    lon_diffs = lon_grid[indexes] - target_lon
    lat_diffs = lat_grid[indexes] - target_lat
    distances = N.sqrt( (lon_diffs * lon_diffs) + (lat_diffs * lat_diffs) )
    indx = N.where(distances == distances.min())[0][0]
    return indexes[0][indx], indexes[1][indx]

def indexesOfNeighborNodes(target_lon, target_lat, relative_nodes,
                           lon_grid, lat_grid, radius):
    y, x = indexOfClosestNode(target_lon,target_lat,lon_grid,lat_grid,radius)

    if relative_nodes in (9,13,25):
        y_indexes = [y+offset for offset in RELATIVE_INDEXES[relative_nodes][0]]
        x_indexes = [x+offset for offset in RELATIVE_INDEXES[relative_nodes][1]]

    else: # smallest number that encloses the target point
        near_lon = lon_grid[y,x]
        near_lat = lat_grid[y,x]

        if target_lon < near_lon: # east half of grid
            if target_lat < near_lat: # north east quadrant of grid
                y_indexes = [y, y, y-1, y-1]
                x_indexes = [x-1, x, x, x-1]
            elif target_lat > near_lat: # south east quadrant of grid
                y_indexes = [y+1, y+1, y, y]
                x_indexes = [x+1, x, x, x+1]
            else: # on east/west edge of two grids
                y_indexes = [ y+closest_y for y in RELATIVE_INDEXES[9][0] ]
                x_indexes = [ x+closest_x for x in RELATIVE_INDEXES[9][1] ]

        elif target_lon > near_lon: # west half of grid
            if target_lat < near_lat: # north west quadrant of grid
                y_indexes = [y, y, y-1, y-1]
                x_indexes = [x, x+1, x+1, x]
            elif target_lat > near_lat: # south west quadrant of grid
                y_indexes = [y+1, y+1, y, y]
                x_indexes = [x, x+1, x+1, x]
            else: # on east/west edge of two grids
                y_indexes = [ y+closest_y for y in RELATIVE_INDEXES[9][0] ]
                x_indexes = [ x+closest_x for x in RELATIVE_INDEXES[9][1] ]

        else: # on north/south edge of two grids
            y_indexes = [ y+closest_y for y in RELATIVE_INDEXES[9][0] ]
            x_indexes = [ x+closest_x for x in RELATIVE_INDEXES[9][1] ]

    return [ y_indexes, x_indexes ]

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def nodesInBBox(bbox, lons, lats, grid):
    max_lon = max(bbox[0], bbox[2])
    min_lon = min(bbox[0], bbox[2])
    max_lat = max(bbox[1], bbox[3])
    min_lat = min(bbox[1], bbox[3])
    indexes = N.where( (lons >= min_lon) & (lats >= min_lat) &
                        (lons <= max_lon) & (lats <= max_lat) )

    # i/x is in indexes[0], j/y is in indexes[1]
    indx_box = (min(indexes[0]), max(indexes[0])+1,
                min(indexes[1]), max(indexes[1])+1)
    if len(grid.shape) == 2:
        return grid[indx_box[0]:indx_box[1],indx_box[2]:indx_box[3]]
    return grid[:,indx_box[0]:indx_box[1],indx_box[2]:indx_box[3]]

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def allQuadrants(point_lon, point_lat, grid_lons, grid_lats):
    """ Verify that there is at least one grid point in each quadrant
    surrounding the point.
    """
    lon_diffs = grid_lons - point_lon
    lat_diffs = grid_lats - point_lat

    quadrants =                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 N.zeros(grid_lons.shape)
    quadrants[ N.where((lon_diffs > 0.) & (lat_diffs > 0.)) ] = 1
    quadrants[ N.where((lon_diffs > 0.) & (lat_diffs < 0.)) ] = 2
    quadrants[ N.where((lon_diffs < 0.) & (lat_diffs < 0.)) ] = 3
    quadrants[ N.where((lon_diffs < 0.) & (lat_diffs > 0.)) ] = 4
    coverage = N.unique(quadrants)

    return 1 in coverage and 2 in coverage and 3 in coverage and 4 in coverage

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def calculateDistances(locus_lon, locus_lat, target_lons, target_lats):
    """ calculate distance (in decimal degrees) between locus node and each
    coordinate pair
    """
    # use numpy for all calculations so that all array indexing is done in "C"
    lon_diffs = locus_lon - target_lons
    lat_diffs = locus_lat - target_lats
    sum_of_squares = (lon_diffs*lon_diffs) + (lat_diffs*lat_diffs)
    #del lon_diffs, lat_diffs
    distances = N.sqrt(sum_of_squares)
    return distances

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def quadrantAndDistance(point_lon, point_lat, grid_lon, grid_lat):
    """ Verify that there is at least one grid point in each quadrant
    surrounding the point.
    """
    lon_diff = grid_lon - point_lon
    lat_diff = grid_lat - point_lat

    distance = math.sqrt( (abs(lon_diff) ** 2.) + (abs(lat_diff) ** 2.) )

    if lon_diff > 0.:
        if lat_diff > 0.: return 1, distance
        if lat_diff < 0.: return 2, distance
    elif lon_diff < 0.:
        if lat_diff < 0.: return 3, distance
        if lat_diff > 0.: return 4, distance
    return 0, distance

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def validValues(base_lon, base_lat, lons, lats, values,
                limit_value_extremes=False):

    # eliminate all nan, inf and ninf values
    indexes = N.where(N.isfinite(values))
    if len(indexes) > 0:
        valid_lons = lons[indexes]
        valid_lats = lats[indexes]
        valid_values = values[indexes]

        # limit values to +- 2 standard deviations from mean
        if len(values) > 5 and limit_value_extremes:
            mean = N.mean(valid_values)
            std_dev = N.std(valid_values)
            plus_2std = mean + (std_dev*2.)
            minus_2std = mean - (std_dev*2.)
            inside = N.where( (valid_values >= min(plus_2std,minus_2std))
                            & (valid_values <= max(plus_2std,minus_2std)) )
            valid_values = valid_values[inside]
            valid_lons = valid_lons[inside]
            valid_lats = valid_lats[inside]
    else:
        valid_lons = [ ]
        valid_lats = [ ]
        valid_values = [ ]

    return valid_lons, valid_lats, valid_values

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def validValueAndClosest(base_lon, base_lat, lons, lats, values,
                         limit_value_extremes=False):
    
    # eliminate all nan, inf and ninf values
    indexes = N.where(N.isfinite(values))

    if len(indexes) > 0:
        lons = lons[indexes]
        lats = lats[indexes]
        values = values[indexes]

        # limit values to +- 2 standard deviations from mean
        if len(values) > 5 and limit_value_extremes:
            mean = N.mean(values)
            std_dev = N.std(values)
            plus_2std = mean + (std_dev*2.)
            minus_2std = mean - (std_dev*2.)
            inside = N.where( (values >= min(plus_2std,minus_2std))
                            & (values <= max(plus_2std,minus_2std)) )
            values = values[inside]
            lons = lons[inside]
            lats = lats[inside]

        # find closest node
        lon_diffs = base_lon - lons
        lat_diffs = base_lat - lats
        distances = N.sqrt( (lon_diffs * lon_diffs) + (lat_diffs * lat_diffs) )
        closest = N.where(distances == distances.min())[0][0]
        return closest, distances, lons, lats, values 

    else:
        return None

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def validValueAndCoverage(base_lon, base_lat, lons, lats, values, vicinity,
                          limit_value_extremes=False):
    in_vicinity = 0
    quadrants = [False,False,False,False]
    valid_lons = [ ]
    valid_lats = [ ]
    valid_values = [ ]

    # eliminate all nan, inf and ninf values
    indexes = N.where(N.isfinite(values))
    if len(indexes) > 0:
        lons = lons[indexes]
        lats = lats[indexes]
        values = values[indexes]

        # limit values to +- 2 standard deviations from mean
        if len(values) > 5 and limit_value_extremes:
            mean = N.mean(values)
            std_dev = N.std(values)
            plus_2std = mean + (std_dev*2.)
            minus_2std = mean - (std_dev*2.)
            inside = N.where( (values >= min(plus_2std,minus_2std))
                            & (values <= max(plus_2std,minus_2std)) )
            values = values[inside]
            lons = lons[inside]
            lats = lats[inside]

        # limit to locations with valid data values
        for indx in range(len(values)):
            value = values[indx]
            lon = lons[indx]
            lat = lats[indx]
            q, d = quadrantAndDistance(base_lon, base_lat, lon, lat)
            if q != 0:
                quadrants[q-1] = True
                valid_lons.append(lon)
                valid_lats.append(lat)
                valid_values.append(value)
            if abs(d) <= vicinity: in_vicinity += 1

    return (quadrants.count(True), in_vicinity, N.array(valid_lons,dtype=float),
            N.array(valid_lats,dtype=float), N.array(valid_values,dtype=float))

