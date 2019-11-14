""" This is the Cython version of interpolation functions used for grid
smoothing.
"""
cimport cython
from libc.math cimport sqrt

import numpy as N
cimport numpy as N

from scipy import linalg

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# turns off bounds checking to speed up indexing loops
@cython.boundscheck(False)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def idw(double unknown_x, double unknown_y,
        N.ndarray[N.float64_t] known_x_coords,
        N.ndarray[N.float64_t] known_y_coords,
        N.ndarray[N.float64_t] known_values):
    """ Simple Inverse Distance Weighted Average from irregularly spaced
    points.

    arguments:
    ---------
        unknown_x      : x coordinate of location with missing data
        unknown_y      : y coordinate of location with missing data
        known_x_coords : 1D array of x coordinates of points with known values 
        known_y_coords : 1D array of y coordinates of points with known values
        known_values   : 1D array if known values at each x,y point
        
    returns:
    -------
        interpolated value at unknown_x, unkown_y
    
    Adapted from IDW_INTERP_ROUTINE originally written by Brian Belcher and
    Laura Joseph.
    """
    cdef double x_diff 
    cdef double y_diff 
    cdef double squares 
    cdef double distance
    cdef double dist_sq
    cdef double value

    # num_known = number of observations 
    cdef int num_known = known_values.shape[0]
    # x-coordinates of the observations
    cdef N.ndarray[N.float64_t] x = known_x_coords
    # y-coordinates of the observations
    cdef N.ndarray[N.float64_t] y = known_y_coords

    cdef double numerator = 0.
    cdef double denominator = 0.

    for i in range(num_known):
        value = known_values[i]
        if N.isfinite(value):
            x_diff = unknown_x - x[i]
            y_diff = unknown_y - y[i]
            squares = (x_diff*x_diff) + (y_diff*y_diff)
            distance = sqrt(squares)
            if distance > 0.:
                dist_sq = distance * distance
                numerator += value / dist_sq
                denominator += 1. / dist_sq

    if denominator != 0.:
        estimate = numerator / denominator
    else:
        estimate = N.inf
    return estimate

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def mq(double unknown_x, double unknown_y,
       N.ndarray[N.float64_t] known_x_coords,
       N.ndarray[N.float64_t] known_y_coords,
       N.ndarray[N.float64_t] known_values, double c_param,
       double smooth_lambda=0.0025, double mean_error=0.5):
    """ Multiquadric Interpolation of value from irregularly spaced points

    arguments:
    ---------
        unknown_x      : x coordinate of location with missing data
        unknown_y      : y coordinate of location with missing data
        known_x_coords : 1D array of x coordinates of points with known values 
        known_y_coords : 1D array of y coordinates of points with known values
        known_values   : 1D array if known values at each x,y point
        c_param        : multiquadric shape parameter (arbitrary small constant
                         that makes the basis function infinitely integrable)
        smooth_lambda  : smoothing parameter, original value = 0.0025
        mean_error     : mean error value for the variable being analyzed.
                         Results are not very sensitive to this parameter, 
                         but results must be in the ballpark.
                             mean_error for unit square = 0.05
                             mean_error for temperature = 0.5
        
    returns:
    -------
        interpolated value at unknown_x, unkown_y
    
    Uses the Multiquadric Interpolation methods presented by Nuss and Titlety
    (MWR 1994).

    Adapted from MQ_INTERP_ROUTINE originally written by Brian Belcher and
    Laura Joseph.
    """
    cdef int i,j # necessary for bounds cehcking speed-up

    cdef int x_xize = 1
    cdef int y_size = 1
    #max_dim = max(IX,JY)
    cdef double x_diff # used in algorithm to generste Qij and Qgi matrices
    cdef double y_diff # used in algorithm to generste Qij and Qgi matrices
    cdef double squares # used in algorithm to generste Qij and Qgi matrices
    cdef double distance_factor # used in algorithm to generste Qi matrix

    cdef double c_sq = c_param * c_param

    # num_known = number of observations 
    cdef int num_known = known_values.shape[0]
    # x-coordinates of the observations
    cdef N.ndarray[N.float64_t] x = known_x_coords
    # y-coordinates of the observations
    cdef N.ndarray[N.float64_t] y = known_y_coords

    # HjT: transposed vector of observations
    cdef N.ndarray[N.float64_t] HjT = N.transpose(known_values)

    # Fill the Qij matrix
    cdef N.ndarray[N.float64_t, ndim=2] Qij = N.empty((num_known,num_known),
                                                      dtype=N.float64)
    cdef double factor
    for j in range(num_known):
        for i in range(num_known):
            #factor = ( -1.0 * math.sqrt(((math.pow(math.fabs(x[j]-x[i]),2) +
            #           math.pow(math.fabs(y[j]-y[i]),2)) / c_sq) + 1.0) )
            x_diff = x[j] - x[i]
            y_diff = y[j] - y[i]
            squares = (x_diff*x_diff) + (y_diff*y_diff)
            distance_factor = -1.0 * sqrt((squares / c_sq) + 1.0)
            # Account for observational uncertainty
            if i == j:
                distance_factor += (num_known * smooth_lambda * mean_error)

            Qij[j][i] = distance_factor

    # Find the inverse of Qij (Qij_inv)
    cdef N.ndarray[N.float64_t, ndim=2] Qij_inv = linalg.inv(Qij)

    # Fill the Qgi matrix
    cdef N.ndarray[N.float64_t] Qgi_matrix = N.empty((num_known,),
                                                      dtype=N.float64)
    for i in range(num_known):
        #Qgi_matrix[i] = ( -1.0 *
        #                  math.sqrt(((math.pow(math.fabs(unknown_x-x[i]),2) +
        #                  math.pow(math.fabs(unknown_y-y[i]),2)) / c_sq) + 1.0)
        x_diff = unknown_x-x[i]
        y_diff = unknown_y-y[i]
        squares = (x_diff*x_diff) + (y_diff*y_diff)
        Qgi_matrix[i] = -1.0 * sqrt((squares / c_sq) + 1.0)

    # Multiply Qij_inv and Hj (determine ALPHAi)
    cdef N.ndarray[N.float64_t] ALPHAi = N.dot(Qij_inv, HjT)

    # Multiply Qgi and ALPHAi (determine Hg)
    cdef N.float64_t Hg = N.dot(Qgi_matrix, ALPHAi)
    return Hg
    #interpolated = N.reshape(Hg,(x_size,y_size))[0][0]
    #return interpolated
    
