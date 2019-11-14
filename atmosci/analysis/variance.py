
import numpy as N

def variance(array1, array2):
    nsize = len(narray)
    var_array = N.empty(nsize)
    i = 0
    while i < nsize:
        var_array[i] = (array1 - array2)**2
        i+=1
    return var_array

