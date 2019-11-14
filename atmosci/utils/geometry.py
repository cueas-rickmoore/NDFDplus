""" Geometry functions
"""
import numpy as N

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def calculateDistances(locus_x, locus_y, x_array, y_array):
    x_diffs = locus_x - x_array
    y_diffs = locus_y - y_array
    sum_of_squares = (x_diffs*x_diffs) + (y_diffs*y_diffs)
    distances = N.sqrt(sum_of_squares)
    return distances

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Determines whether two circles collide and, if applicable,
# the points at which their borders intersect.
# Based on an algorithm described by Paul Bourke:
# http://local.wasp.uwa.edu.au/~pbourke/geometry/2circle/
# Arguments:
#   P0 (complex): the centre point of the first circle
#   P1 (complex): the centre point of the second circle
#   r0 (numeric): radius of the first circle
#   r1 (numeric): radius of the second circle
# Returns:
#   False if the circles do not collide
#   True if one circle wholly contains another such that the borders
#       do not overlap, or overlap exactly (e.g. two identical circles)
#   An array of two complex numbers containing the intersection points
#       if the circle's borders intersect.
def CirclesIntersect(P0, P1, r0, r1):
    from math import sqrt

    if type(P0) != complex or type(P1) != complex:
        raise TypeError("P0 and P1 must be complex types")
    # d = distance
    d = sqrt((P1.real - P0.real)**2 + (P1.imag - P0.imag)**2)
    # n**2 in Python means "n to the power of 2"
    # note: d = a + b

    if d > (r0 + r1):
        return False
    elif d < abs(r0 - r1):
        return True
    elif d == 0:
        return True
    else:
        a = (r0**2 - r1**2 + d**2) / (2 * d)
        b = d - a
        h = sqrt(r0**2 - a**2)
        P2 = P0 + a * (P1 - P0) / d

        i1x = P2.real + h * (P1.imag - P0.imag) / d
        i1y = P2.imag - h * (P1.real - P0.real) / d
        i2x = P2.real - h * (P1.imag - P0.imag) / d
        i2y = P2.imag + h * (P1.real - P0.real) / d

        i1 = complex(i1x, i1y)
        i2 = complex(i2x, i2y)

        return [i1, i2]

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def pointsToPolygon(lons, lats):
    from shapely.geometry import MultiPoint
    points = MultiPoint([(lons[i],lats[i]) for i in range(len(lons))])
    return points.convex_hull

